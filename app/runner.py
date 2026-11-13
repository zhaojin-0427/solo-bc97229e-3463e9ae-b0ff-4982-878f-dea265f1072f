"""批次执行器：在独立目录中以后台子进程运行承包商脚本。

关键约定
--------
* 每批次一个独立目录，脚本以输入、输出文件路径为两个参数运行。
* 主脚本在独立进程组（新会话）中启动，其派生的普通子进程同组，
  超时 / 取消时对整组发 SIGKILL，杜绝孤儿子进程继续写文件。
* 只有同时满足：
  1) 主脚本退出码为 0；
  2) 进程组内已无任何存活的普通子进程；
  3) 输出文件通过完整校验；
  才把批次从 running 原子翻转为 succeeded（可下载）。
  父进程先退出、子进程仍在写文件时不会提前发布。
* 执行管控不依赖数据库可写：另一连接持续持有 SQLite 写锁时，
  到期批次仍按超时上限整组终止、残件立即清除；终态登记进入持久队列，
  由登记线程自动重试，解锁后接续落库（超时→timeout、失败→failed、
  校验通过→succeeded）。已发布结果不回退、脚本不重跑、
  未发布批次不提供残件下载。
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

from . import config, db, validation

# 轮询间隔：进程退出后给子进程一点收尾时间，随后确认是否整组退出
_POLL_INTERVAL = 0.1
# 主进程退出后，等待普通子进程自行结束的宽限秒数；超过则整组杀掉
_ORPHAN_GRACE = 1.0
# 杀进程组后等待回收的秒数
_KILL_WAIT = 5.0


def _log_runner(batch_dir: Path, message: str) -> None:
    line = f"[{time.strftime('%Y-%m-%dT%H:%M:%S')}] {message}\n"
    with open(batch_dir / config.RUN_LOG_NAME, "a", encoding="utf-8") as fh:
        fh.write(line)


def _group_pids(pgid: int) -> list[int]:
    """返回进程组内当前存活的全部 PID（含主脚本及其普通子进程）。

    僵尸进程（state == 'Z'）已死、不再执行，视为不存活，
    但仍需其父进程 wait 回收，调用方负责 reaper。
    """
    found: list[int] = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        pid = int(entry)
        try:
            with open(f"/proc/{pid}/stat", "r", encoding="ascii") as fh:
                stat = fh.read().split()
            # /proc/<pid>/stat：第 3 字段为状态，第 5 字段为 pgrp
            state = stat[2]
            pgrp = int(stat[4])
        except (FileNotFoundError, ProcessLookupError, ValueError, IndexError):
            continue
        if pgrp == pgid and state != "Z":
            found.append(pid)
    return found


def _kill_group(pgid: int, batch_dir: Path, reason: str, proc: subprocess.Popen) -> None:
    """终止该批次实际执行的主进程及全部子进程，并回收。"""
    pids = _group_pids(pgid)
    _log_runner(batch_dir, f"{reason}：向进程组 {pgid} 发送 SIGKILL，成员={pids}")
    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except OSError as exc:
        _log_runner(batch_dir, f"killpg 失败: {exc!r}")
    deadline = time.monotonic() + _KILL_WAIT
    while time.monotonic() < deadline:
        remaining = _group_pids(pgid)
        if not remaining:
            break
        time.sleep(0.05)
    else:
        # 兜底逐个杀
        for pid in _group_pids(pgid):
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
    # 回收主脚本僵尸，避免长期占用 /proc 项
    try:
        proc.wait(timeout=_KILL_WAIT)
    except subprocess.TimeoutExpired:
        _log_runner(batch_dir, "警告：主进程在 SIGKILL 后仍未被回收")


def _cleanup_remnants(batch_dir: Path) -> None:
    """清除残件：输出文件与 work 目录（成功批次在发布后才调用则不删输出）。"""
    output = batch_dir / config.OUTPUT_CSV_NAME
    work = batch_dir / config.WORK_DIR_NAME
    try:
        if output.exists():
            output.unlink()
    except OSError:
        pass
    try:
        if work.exists():
            shutil.rmtree(work, ignore_errors=True)
    except OSError:
        pass


def _on_terminal_applied(
    batch_id: str, batch_dir: Path, status: str, rowcount: int
) -> None:
    """终态登记落库后的回调（在登记线程中执行）。

    rowcount == 0 表示条件更新未命中：取消已先受理（或行已处其它终态）。
    此时绝不覆盖既有终态；若发布输给了取消，清除输出残件并为 cancelled
    批次补写终态字段，保证取消批次字段完整、不可下载。
    """
    if rowcount:
        _log_runner(batch_dir, f"批次终态={status} 已落库")
        return
    try:
        row = db.query_one(
            "SELECT status, finished_at FROM batches WHERE id = ?", (batch_id,)
        )
    except Exception:
        return
    if row is None:
        return
    if row["status"] == config.STATUS_CANCELLED:
        _cleanup_remnants(batch_dir)  # 发布未生效：输出残件不得留存
        if row["finished_at"] is None:
            db.execute_durable(
                """
                UPDATE batches
                   SET finished_at = ?, detail = ?
                 WHERE id = ? AND status = ? AND finished_at IS NULL
                """,
                (
                    db.utcnow(),
                    "取消先于发布受理，批次不发布，残件已清除",
                    batch_id,
                    config.STATUS_CANCELLED,
                ),
            )
        _log_runner(batch_dir, "取消先于发布生效，终态登记未覆盖 cancelled")


def _finish(
    batch_id: str,
    status: str,
    *,
    exit_code: Optional[int],
    output_rows: Optional[int],
    detail: str,
    keep_output: bool,
    batch_dir: Path,
    expect_status: str = config.STATUS_RUNNING,
) -> None:
    """登记终态：残件立即清理（不等数据库），状态翻转进入持久登记队列。

    条件 UPDATE（WHERE status = expect_status）在真正落库时才求值，
    因此与取消接口的互斥语义与同步写入完全一致：谁先提交谁生效。
    登记遇写锁由后台线程自动重试，解锁后接续落库，本线程即刻返回。
    finished_at 取决策完成时刻（而非落库时刻），如实记录执行结束时间。
    """
    if not keep_output:
        _cleanup_remnants(batch_dir)
    output_path = str(batch_dir / config.OUTPUT_CSV_NAME) if keep_output else None
    db.execute_durable(
        """
        UPDATE batches
           SET status = ?, finished_at = ?, exit_code = ?,
               output_rows = ?, output_path = ?, detail = ?
         WHERE id = ? AND status = ?
        """,
        (
            status,
            db.utcnow(),
            exit_code,
            output_rows,
            output_path,
            detail[: config.MAX_DETAIL_CHARS],
            batch_id,
            expect_status,
        ),
        on_applied=lambda rowcount: _on_terminal_applied(
            batch_id, batch_dir, status, rowcount
        ),
    )
    _log_runner(
        batch_dir, f"批次终态={status} 登记已受理（等待落库） detail={detail[:200]!r}"
    )


def run_batch(batch_id: str, expected_hole_ids: list[str]) -> None:
    """在后台线程中执行一个批次（由 API 提交后触发）。"""
    row = db.query_one("SELECT * FROM batches WHERE id = ?", (batch_id,))
    if row is None:
        return
    batch_dir = Path(row["dir_path"])
    input_path = Path(row["input_path"])
    script_path = Path(row["script_path"])
    work_dir = Path(row["work_dir"])
    output_path = batch_dir / config.OUTPUT_CSV_NAME
    stdout_log = open(batch_dir / config.STDOUT_LOG_NAME, "wb")
    stderr_log = open(batch_dir / config.STDERR_LOG_NAME, "wb")

    deadline = time.monotonic() + config.BATCH_TIMEOUT_SECONDS
    _log_runner(batch_dir, f"启动脚本 {script_path}，超时 {config.BATCH_TIMEOUT_SECONDS}s")

    # -I：隔离模式，不把脚本目录 / cwd 加入 sys.path，防止上传目录中的
    # 同名模块（如 json.py）被当作标准库导入；PYTHONSAFEPATH 双保险。
    env = os.environ.copy()
    env["PYTHONSAFEPATH"] = "1"
    try:
        proc = subprocess.Popen(
            [
                sys.executable,
                "-I",
                str(script_path),
                str(input_path),
                str(output_path),
            ],
            cwd=str(work_dir),
            stdin=subprocess.DEVNULL,
            stdout=stdout_log,
            stderr=stderr_log,
            env=env,
            start_new_session=True,  # 新进程组/新会话，便于整组清理
        )
    except OSError as exc:
        stdout_log.close()
        stderr_log.close()
        _finish(
            batch_id,
            config.STATUS_FAILED,
            exit_code=None,
            output_rows=None,
            detail=f"无法启动脚本进程: {exc!r}",
            keep_output=False,
            batch_dir=batch_dir,
        )
        return

    pgid = proc.pid
    orphan_deadline: Optional[float] = None
    registered = False  # 是否已提交终态登记（提交后线程即可安全退出）

    try:
        while True:
            # 取消检查：走独立只读连接，外部写锁不会阻塞；即便如此仍按
            # 尽力而为处理——读库异常绝不拖延下面的超时执法。
            try:
                current = db.query_one(
                    "SELECT status FROM batches WHERE id = ?", (batch_id,)
                )
            except Exception:
                current = None
            if current is not None and current["status"] == config.STATUS_CANCELLED:
                _kill_group(pgid, batch_dir, "收到取消", proc)
                _finish(
                    batch_id,
                    config.STATUS_CANCELLED,
                    exit_code=None,
                    output_rows=None,
                    detail="批次已取消：执行进程及全部子进程已终止，残件已清除",
                    keep_output=False,
                    batch_dir=batch_dir,
                    expect_status=config.STATUS_CANCELLED,
                )
                registered = True
                return
            # 超时检查：只依赖单调时钟，与数据库是否可写无关
            if time.monotonic() >= deadline:
                _kill_group(pgid, batch_dir, "执行超时", proc)
                _finish(
                    batch_id,
                    config.STATUS_TIMEOUT,
                    exit_code=None,
                    output_rows=None,
                    detail=(
                        f"执行超过 {config.BATCH_TIMEOUT_SECONDS:.0f} 秒："
                        "进程组已终止，残件已清除"
                    ),
                    keep_output=False,
                    batch_dir=batch_dir,
                )
                registered = True
                return

            ret = proc.poll()
            if ret is not None:
                # 主脚本已退出。必须确认进程组内没有仍存活的普通子进程，
                # 否则等待（父先退出、子持续写文件时绝不提前发布）。
                remaining = _group_pids(pgid)
                if remaining:
                    if orphan_deadline is None:
                        orphan_deadline = time.monotonic() + _ORPHAN_GRACE
                        _log_runner(
                            batch_dir,
                            f"主进程已退出(code={ret})，仍有子进程 {remaining}，等待整组退出",
                        )
                    if time.monotonic() >= orphan_deadline:
                        _kill_group(pgid, batch_dir, "子进程在宽限期后仍存活", proc)
                        _finish(
                            batch_id,
                            config.STATUS_FAILED,
                            exit_code=ret,
                            output_rows=None,
                            detail=(
                                f"主脚本退出码 {ret}，但普通子进程 {remaining} "
                                "未随父进程退出，已整组终止并判定失败"
                            ),
                            keep_output=False,
                            batch_dir=batch_dir,
                        )
                        registered = True
                        return
                    time.sleep(_POLL_INTERVAL)
                    continue
                # 整组退出，进入退出码 + 输出校验
                _evaluate(batch_id, ret, expected_hole_ids, batch_dir, output_path)
                registered = True
                return

            time.sleep(_POLL_INTERVAL)
    finally:
        stdout_log.close()
        stderr_log.close()
        # 任何意外路径下都不允许留下存活进程组
        try:
            if proc.poll() is None or _group_pids(pgid):
                _kill_group(pgid, batch_dir, "执行器收尾", proc)
        except Exception:
            pass
        if not registered:
            # 不应到达的异常路径：保守判失败，绝不发布；登记同样走持久队列
            try:
                _cleanup_remnants(batch_dir)
                _finish(
                    batch_id,
                    config.STATUS_FAILED,
                    exit_code=proc.returncode,
                    output_rows=None,
                    detail="执行器异常收尾，未完成发布判定",
                    keep_output=False,
                    batch_dir=batch_dir,
                )
            except Exception:
                pass


def _evaluate(
    batch_id: str,
    exit_code: int,
    expected_hole_ids: list[str],
    batch_dir: Path,
    output_path: Path,
) -> None:
    """主脚本与全部子进程退出后：核对退出码与输出，再决定是否发布。"""
    if exit_code != 0:
        detail = f"主脚本退出码为 {exit_code}（非 0），不发布输出"
        try:
            err = (batch_dir / config.STDERR_LOG_NAME).read_text(
                encoding="utf-8", errors="replace"
            )
            if err.strip():
                detail += f"；stderr: {err.strip()}"
        except OSError:
            pass
        _finish(
            batch_id,
            config.STATUS_FAILED,
            exit_code=exit_code,
            output_rows=None,
            detail=detail,
            keep_output=False,
            batch_dir=batch_dir,
        )
        return

    if not output_path.exists():
        _finish(
            batch_id,
            config.STATUS_FAILED,
            exit_code=exit_code,
            output_rows=None,
            detail="主脚本退出码为 0，但未生成输出文件",
            keep_output=False,
            batch_dir=batch_dir,
        )
        return

    try:
        text = output_path.read_text(encoding="utf-8-sig")
    except OSError as exc:
        _finish(
            batch_id,
            config.STATUS_FAILED,
            exit_code=exit_code,
            output_rows=None,
            detail=f"读取输出文件失败: {exc!r}",
            keep_output=False,
            batch_dir=batch_dir,
        )
        return

    try:
        out_rows = validation.validate_output_csv(text, expected_hole_ids)
    except validation.ValidationError as exc:
        _finish(
            batch_id,
            config.STATUS_FAILED,
            exit_code=exit_code,
            output_rows=None,
            detail=f"输出校验失败: {exc}",
            keep_output=False,
            batch_dir=batch_dir,
        )
        return

    # 全部条件满足：原子翻转为 succeeded，输出文件保留并可下载
    _finish(
        batch_id,
        config.STATUS_SUCCEEDED,
        exit_code=exit_code,
        output_rows=out_rows,
        detail="退出码 0、全部子进程退出、输出校验通过",
        keep_output=True,
        batch_dir=batch_dir,
    )
