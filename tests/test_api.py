"""端到端测试：启动真实 uvicorn 服务（随机端口）走 HTTP 调用。

覆盖：正常转换、父进程先退出/子进程持续写、10 秒超时、取消与发布竞态、
非法输出、非零退出、输入校验、批次隔离、重启恢复、进程组泄漏检查，
以及「另一连接持续持有 SQLite 写锁」时跨批完成、写锁等待与执行到期
交错故障链的回归测试（test_14）。
"""

from __future__ import annotations

import os
import select
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path

import httpx
import uvicorn

# 在导入应用前指定隔离的数据根目录
_TMP_ROOT = tempfile.mkdtemp(prefix="drill-api-test-")
os.environ["DATA_ROOT"] = _TMP_ROOT
os.environ.setdefault("BATCH_TIMEOUT_SECONDS", "10")

from app import config, db, runner  # noqa: E402
from app.main import app  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
EXAMPLES = REPO_ROOT / "examples"

SAMPLE_CSV = (EXAMPLES / "sample_input.csv").read_bytes()
NORMAL_SCRIPT = (EXAMPLES / "normal_transform.py").read_bytes()
PARENT_EXIT_SCRIPT = (EXAMPLES / "parent_exits_child_writes.py").read_bytes()
SLOW_SCRIPT = (EXAMPLES / "slow_loop.py").read_bytes()
INVALID_SCRIPT = (EXAMPLES / "invalid_output.py").read_bytes()

EXIT_3_SCRIPT = b"import sys\nsys.exit(3)\n"

# 延迟 2 秒再产出合法输出：保证批次在写锁持有期间才完成执行
DELAYED_TRANSFORM_SCRIPT = b"""
import csv, sys, time
time.sleep(2)
with open(sys.argv[1], newline="", encoding="utf-8-sig") as fh:
    rows = list(csv.reader(fh))
with open(sys.argv[2], "w", newline="", encoding="utf-8") as fh:
    writer = csv.writer(fh)
    writer.writerow(["hole_id", "depth_m", "grade_pct"])
    for hole_id, depth_raw, grade_raw in rows[1:]:
        writer.writerow([hole_id, f"{float(depth_raw):.4f}", f"{float(grade_raw):.2f}"])
"""

# 延迟 2 秒再以非零退出：保证失败登记发生在写锁持有期间
DELAYED_EXIT_3_SCRIPT = b"import sys, time\ntime.sleep(2)\nsys.exit(3)\n"

# 主脚本派生一个普通子进程（同进程组）后一起长眠：跑满 10 秒超时。
# 子进程命令行带上输入文件路径（位于测试数据根目录下），便于泄漏检测发现。
SPAWN_CHILD_SLEEP_SCRIPT = b"""
import subprocess, sys, time
child = subprocess.Popen(
    [sys.executable, "-c", "import time; time.sleep(60)", sys.argv[1]]
)
print("child pid:", child.pid, flush=True)
time.sleep(60)
"""

# 外部锁持有者：独立进程对同一 SQLite 库 BEGIN IMMEDIATE 并持续持有写锁，
# 直到从 stdin 读到一行才 COMMIT 释放。模拟矿山现场另一连接占住写锁的场景。
LOCK_HOLDER_SCRIPT = """
import sqlite3, sys
conn = sqlite3.connect(sys.argv[1], timeout=30, isolation_level=None)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("BEGIN IMMEDIATE")
conn.execute("UPDATE batches SET detail = detail WHERE id = '__lock_probe__'")
print("LOCKED", flush=True)
sys.stdin.readline()
conn.execute("COMMIT")
print("RELEASED", flush=True)
"""


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class ServerThread:
    """在线程中运行 uvicorn，支持停启以模拟服务重启。"""

    def __init__(self) -> None:
        self.port = free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self._thread: threading.Thread | None = None
        self._server: uvicorn.Server | None = None

    def start(self) -> None:
        cfg = uvicorn.Config(
            app, host="127.0.0.1", port=self.port, log_level="warning", workers=1
        )
        self._server = uvicorn.Server(cfg)
        self._thread = threading.Thread(
            target=self._server.run, name="uvicorn-test", daemon=True
        )
        self._thread.start()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self._server.started:
                return
            time.sleep(0.02)
        raise RuntimeError("测试服务器启动超时")

    def stop(self) -> None:
        assert self._server is not None and self._thread is not None
        self._server.should_exit = True
        self._thread.join(timeout=10)
        self._server = None
        self._thread = None

    def restart(self) -> None:
        self.stop()
        self.start()


def leftover_pids() -> list[tuple[int, list[str]]]:
    """扫描 /proc，找出命令行仍引用测试数据根的存活进程（泄漏检测）。"""
    found = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        pid = int(entry)
        try:
            raw = Path(f"/proc/{pid}/cmdline").read_bytes()
        except OSError:
            continue
        parts = [p for p in raw.split(b"\x00") if p]
        if _TMP_ROOT.encode() in b" ".join(parts):
            found.append((pid, [p.decode(errors="replace") for p in parts]))
    return found


class ApiTestCase(unittest.TestCase):
    server: ServerThread

    @classmethod
    def setUpClass(cls) -> None:
        cls.server = ServerThread()
        cls.server.start()
        cls.client = httpx.Client(base_url=cls.server.base_url, timeout=30)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.close()
        cls.server.stop()
        time.sleep(0.3)
        leaked = leftover_pids()
        db.close()
        shutil.rmtree(_TMP_ROOT, ignore_errors=True)
        assert not leaked, f"测试结束后仍有存活子进程: {leaked}"

    def submit(self, csv_bytes: bytes = SAMPLE_CSV, script_bytes: bytes = NORMAL_SCRIPT,
               script_name: str = "script.py") -> dict:
        resp = self.client.post(
            "/batches",
            files={
                "csv_file": ("input.csv", csv_bytes, "text/csv"),
                "script_file": (script_name, script_bytes, "text/x-python"),
            },
        )
        self.assertEqual(resp.status_code, 202, resp.text)
        return resp.json()

    def wait_for(self, batch_id: str, timeout: float = 20.0) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            resp = self.client.get(f"/batches/{batch_id}")
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            if body["status"] != config.STATUS_RUNNING:
                return body
            time.sleep(0.1)
        self.fail(f"批次 {batch_id} 在 {timeout}s 内未结束")

    def assert_no_leftover_processes(self) -> None:
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            if not leftover_pids():
                return
            time.sleep(0.1)
        self.fail(f"发现残留进程: {leftover_pids()}")

    # ---------- 写锁回归测试辅助 ----------

    def pids_for(self, batch_id: str) -> list[tuple[int, list[str]]]:
        """仍存活且命令行引用指定批次目录的进程（脚本及其普通子进程）。"""
        return [
            (pid, parts)
            for pid, parts in leftover_pids()
            if any(batch_id in part for part in parts)
        ]

    def wait_runner_log(self, batch_id: str, needle: str, timeout: float = 10.0) -> None:
        log = Path(config.BATCHES_ROOT) / batch_id / config.RUN_LOG_NAME
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                if log.is_file() and needle in log.read_text(encoding="utf-8"):
                    return
            except OSError:
                pass
            time.sleep(0.1)
        self.fail(f"批次 {batch_id} 的 runner.log 在 {timeout}s 内未出现 {needle!r}")

    def wait_batch_processes_gone(self, batch_id: str, timeout: float = 10.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not self.pids_for(batch_id):
                return
            time.sleep(0.1)
        self.fail(f"批次 {batch_id} 的进程在 {timeout}s 后仍存活: {self.pids_for(batch_id)}")

    def acquire_write_lock(self) -> subprocess.Popen:
        """启动外部进程持续持有 SQLite 写锁，返回该子进程。"""
        holder = subprocess.Popen(
            [sys.executable, "-I", "-c", LOCK_HOLDER_SCRIPT, str(config.DB_PATH)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        ready, _, _ = select.select([holder.stdout], [], [], 15.0)
        line = holder.stdout.readline().strip() if ready else ""
        self.assertEqual(line, "LOCKED", f"锁持有者未能取得写锁: {line!r}")
        return holder

    def release_write_lock(self, holder: subprocess.Popen) -> None:
        try:
            if holder.stdin:
                holder.stdin.write("\n")
                holder.stdin.flush()
        except (BrokenPipeError, OSError):
            pass
        try:
            holder.wait(timeout=10)
        except subprocess.TimeoutExpired:
            holder.kill()
            holder.wait(timeout=5)
        for pipe in (holder.stdin, holder.stdout, holder.stderr):
            try:
                if pipe:
                    pipe.close()
            except OSError:
                pass

    # ---------- 测试用例 ----------

    def test_01_health_and_list(self) -> None:
        self.assertEqual(self.client.get("/health").json()["status"], "ok")
        resp = self.client.get("/batches")
        self.assertEqual(resp.status_code, 200)
        self.assertIsInstance(resp.json(), list)

    def test_02_normal_transform_and_download(self) -> None:
        batch = self.submit()
        self.assertEqual(batch["status"], "running")
        result = self.wait_for(batch["id"])
        self.assertEqual(result["status"], "succeeded", result["detail"])
        self.assertEqual(result["exit_code"], 0)
        self.assertEqual(result["input_rows"], 6)
        self.assertEqual(result["output_rows"], 6)
        self.assertEqual(result["download_url"], f"/batches/{batch['id']}/download")

        resp = self.client.get(f"/batches/{batch['id']}/download")
        self.assertEqual(resp.status_code, 200)
        lines = resp.text.strip().splitlines()
        self.assertEqual(lines[0], "hole_id,depth_m,grade_pct")
        # 原序 + 重复行保留（ZK-001 重复、ZK-002 重复）
        ids = [line.split(",")[0] for line in lines[1:]]
        self.assertEqual(ids, ["ZK-001", "ZK-002", "ZK-001", "ZK-003", "ZK-004", "ZK-002"])
        self.assertIn("12.5000,1.32", resp.text)

        batch_dir = Path(config.BATCHES_ROOT) / batch["id"]
        self.assertTrue((batch_dir / config.OUTPUT_CSV_NAME).is_file())
        self.assertTrue((batch_dir / config.STDOUT_LOG_NAME).is_file())

    def test_03_parent_exits_child_keeps_writing_not_published(self) -> None:
        start = time.monotonic()
        batch = self.submit(script_bytes=PARENT_EXIT_SCRIPT)
        # 运行中不可下载
        self.assertEqual(self.client.get(f"/batches/{batch['id']}/download").status_code, 409)

        result = self.wait_for(batch["id"], timeout=10)
        elapsed = time.monotonic() - start
        self.assertEqual(result["status"], "failed")
        self.assertIn("子进程", result["detail"])
        # 父进程秒退 + 1 秒宽限后即判失败，远早于子进程的 8 秒和 10 秒超时
        self.assertLess(elapsed, 4.0)

        batch_dir = Path(config.BATCHES_ROOT) / batch["id"]
        self.assertFalse((batch_dir / config.OUTPUT_CSV_NAME).exists())
        self.assertFalse((batch_dir / config.WORK_DIR_NAME).exists())
        self.assert_no_leftover_processes()

    def test_04_timeout_kills_group_and_clears_remnants(self) -> None:
        start = time.monotonic()
        batch = self.submit(script_bytes=SLOW_SCRIPT)
        result = self.wait_for(batch["id"], timeout=20)
        elapsed = time.monotonic() - start
        self.assertEqual(result["status"], "timeout")
        self.assertIn("10", result["detail"])
        self.assertGreaterEqual(elapsed, 9.5)
        self.assertLess(elapsed, 12.0)
        batch_dir = Path(config.BATCHES_ROOT) / batch["id"]
        self.assertFalse((batch_dir / config.OUTPUT_CSV_NAME).exists())
        self.assert_no_leftover_processes()

    def test_05_cancel_accepted_then_no_publish(self) -> None:
        batch = self.submit(script_bytes=SLOW_SCRIPT)
        time.sleep(0.5)
        resp = self.client.post(f"/batches/{batch['id']}/cancel")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["status"], "cancelled")
        result = self.wait_for(batch["id"], timeout=5)
        self.assertEqual(result["status"], "cancelled")
        # 等待执行器完成进程组终止并补写终态字段
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            result = self.client.get(f"/batches/{batch['id']}").json()
            if result["finished_at"] is not None:
                break
            time.sleep(0.1)
        self.assertIsNotNone(result["finished_at"])
        self.assertIn("取消", result["detail"])
        self.assertEqual(self.client.get(f"/batches/{batch['id']}/download").status_code, 409)
        self.assert_no_leftover_processes()

    def test_06_cancel_rejected_after_publish(self) -> None:
        batch = self.submit()
        self.wait_for(batch["id"])
        resp = self.client.post(f"/batches/{batch['id']}/cancel")
        self.assertEqual(resp.status_code, 409)
        # 已发布结果仍可下载
        self.assertEqual(
            self.client.get(f"/batches/{batch['id']}/download").status_code, 200
        )

    def test_07_invalid_output_fails_validation(self) -> None:
        batch = self.submit(script_bytes=INVALID_SCRIPT)
        result = self.wait_for(batch["id"])
        self.assertEqual(result["status"], "failed")
        self.assertIn("校验", result["detail"])
        batch_dir = Path(config.BATCHES_ROOT) / batch["id"]
        self.assertFalse((batch_dir / config.OUTPUT_CSV_NAME).exists())

    def test_08_nonzero_exit_code(self) -> None:
        batch = self.submit(script_bytes=EXIT_3_SCRIPT)
        result = self.wait_for(batch["id"])
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["exit_code"], 3)
        self.assertIn("3", result["detail"])

    def test_09_bad_input_rejected(self) -> None:
        bad = b"hole_id,depth,grade\nZK-1,1,2\n"
        resp = self.client.post(
            "/batches",
            files={
                "csv_file": ("input.csv", bad, "text/csv"),
                "script_file": ("s.py", NORMAL_SCRIPT, "text/x-python"),
            },
        )
        self.assertEqual(resp.status_code, 422)

    def test_10_missing_batch_404(self) -> None:
        bid = "deadbeef" * 4
        self.assertEqual(self.client.get(f"/batches/{bid}").status_code, 404)
        self.assertEqual(self.client.post(f"/batches/{bid}/cancel").status_code, 404)

    def test_11_concurrent_batches_use_isolated_dirs(self) -> None:
        ids = [self.submit()["id"] for _ in range(3)]
        dirs = set()
        for bid in ids:
            result = self.wait_for(bid)
            self.assertEqual(result["status"], "succeeded", result["detail"])
            row = db.query_one("SELECT dir_path FROM batches WHERE id=?", (bid,))
            dirs.add(row["dir_path"])
        self.assertEqual(len(dirs), 3)

    def test_12_timeout_batch_does_not_affect_other_batches(self) -> None:
        slow = self.submit(script_bytes=SLOW_SCRIPT)
        time.sleep(0.2)
        normal = self.submit()
        normal_result = self.wait_for(normal["id"])
        self.assertEqual(normal_result["status"], "succeeded")
        slow_result = self.wait_for(slow["id"], timeout=15)
        self.assertEqual(slow_result["status"], "timeout")
        # 正常批次仍可查询与下载
        self.assertEqual(
            self.client.get(f"/batches/{normal['id']}/download").status_code, 200
        )
        statuses = {b["id"]: b["status"] for b in self.client.get("/batches").json()}
        self.assertEqual(statuses[normal["id"]], "succeeded")
        self.assertEqual(statuses[slow["id"]], "timeout")

    def test_13_restart_keeps_done_marks_running_interrupted(self) -> None:
        done = self.submit()
        done_result = self.wait_for(done["id"])
        self.assertEqual(done_result["status"], "succeeded")

        # 伪造一个重启时仍在 running 的批次记录
        bid = "f" * 32
        fake_dir = Path(config.BATCHES_ROOT) / bid
        fake_dir.mkdir(parents=True, exist_ok=True)
        db.execute(
            "INSERT INTO batches (id, status, created_at, started_at, dir_path, "
            "input_path, script_path, work_dir, input_rows) VALUES (?,?,?,?,?,?,?,?,?)",
            (
                bid, config.STATUS_RUNNING, db.utcnow(), db.utcnow(), str(fake_dir),
                str(fake_dir / "input.csv"), str(fake_dir / "script.py"),
                str(fake_dir / "work"), 1,
            ),
        )

        # 真正停掉并重启服务，触发 lifespan 启动恢复
        self.server.restart()

        interrupted = self.client.get(f"/batches/{bid}").json()
        self.assertEqual(interrupted["status"], "interrupted")
        self.assertEqual(self.client.get(f"/batches/{bid}/download").status_code, 409)
        # 已完成结果保留且仍可下载
        still_done = self.client.get(f"/batches/{done['id']}").json()
        self.assertEqual(still_done["status"], "succeeded")
        self.assertEqual(
            self.client.get(f"/batches/{done['id']}/download").status_code, 200
        )

    def test_14_writelock_completion_timeout_interleaving(self) -> None:
        """回归：另一连接持续持有 SQLite 写锁时——

        * 到期批次仍按 10 秒整组终止（脚本 + 普通子进程），残件清除，
          执行管控不被其它批次的终态登记拖住；
        * 查询接口始终可读取完整已提交状态，不被写锁卡住；
        * 完成批次的终态登记遇锁自动接续：解锁后 校验通过→succeeded、
          非零退出→failed、执行到期→timeout；
        * 已发布结果不回退，未发布批次不提供残件下载，脚本不重跑。
        """
        # 0) 先发布一个批次：写锁期间与解锁后都不得回退
        done = self.submit()
        self.assertEqual(self.wait_for(done["id"])["status"], "succeeded")

        # 1) 提交三个批次，它们的完成/到期都将落在写锁持有期间
        ok = self.submit(script_bytes=DELAYED_TRANSFORM_SCRIPT)    # 2s 后完成，应 succeeded
        boom = self.submit(script_bytes=DELAYED_EXIT_3_SCRIPT)     # 2s 后退出码 3，应 failed
        slow = self.submit(script_bytes=SPAWN_CHILD_SLEEP_SCRIPT)  # 跑满 10s，应 timeout
        slow_dir = Path(config.BATCHES_ROOT) / slow["id"]
        slow_started = time.monotonic()

        # 2) 外部连接取得并持续持有写锁
        holder = self.acquire_write_lock()
        try:
            # 3) ok / boom 在锁期间执行完毕，终态登记已受理但被写锁挡住
            self.wait_runner_log(ok["id"], "登记已受理")
            self.wait_runner_log(boom["id"], "登记已受理")
            self.wait_batch_processes_gone(ok["id"])
            self.wait_batch_processes_gone(boom["id"])

            # 4) 查询不卡：列表/单批查询快速返回完整已提交状态
            t0 = time.monotonic()
            listed = self.client.get("/batches").json()
            self.assertLess(time.monotonic() - t0, 3.0, "写锁期间批次列表查询被卡住")
            t0 = time.monotonic()
            self.assertEqual(self.client.get(f"/batches/{ok['id']}").status_code, 200)
            self.assertLess(time.monotonic() - t0, 3.0, "写锁期间单批查询被卡住")
            statuses = {b["id"]: b["status"] for b in listed}
            self.assertEqual(statuses[done["id"]], "succeeded")  # 已发布不回退
            for b in (ok, boom, slow):
                # 终态登记尚未落库，已提交状态仍是 running
                self.assertEqual(statuses[b["id"]], "running")
            # 已发布批次锁期间仍可下载；未发布批次不提供残件下载
            self.assertEqual(
                self.client.get(f"/batches/{done['id']}/download").status_code, 200
            )
            self.assertEqual(
                self.client.get(f"/batches/{ok['id']}/download").status_code, 409
            )
            # 写锁期间取消无法受理（写不可用）→ 503，且不改变后续超时走向
            resp = self.client.post(f"/batches/{slow['id']}/cancel")
            self.assertEqual(resp.status_code, 503, resp.text)
            # 写锁期间建批同样 503，且不得留下残件目录
            dirs_before = set(os.listdir(config.BATCHES_ROOT))
            resp = self.client.post(
                "/batches",
                files={
                    "csv_file": ("input.csv", SAMPLE_CSV, "text/csv"),
                    "script_file": ("script.py", NORMAL_SCRIPT, "text/x-python"),
                },
            )
            self.assertEqual(resp.status_code, 503, resp.text)
            self.assertEqual(
                dirs_before,
                set(os.listdir(config.BATCHES_ROOT)),
                "建批遇锁失败不得留下残件目录",
            )

            # 5) 关键：写锁仍持有，slow 仍须在 ~10s 被整组终止并清除残件
            self.wait_runner_log(slow["id"], "执行超时", timeout=15)
            self.assertGreaterEqual(time.monotonic() - slow_started, 9.5)
            self.assertLess(
                time.monotonic() - slow_started, 12.0,
                "写锁期间到期批次未被准时终止",
            )
            # 终态登记已受理 => 进程组已回收、残件已清理（登记本身等锁挂起）
            self.wait_runner_log(slow["id"], "登记已受理", timeout=5)
            self.wait_batch_processes_gone(slow["id"], timeout=5)
            self.assertFalse((slow_dir / config.OUTPUT_CSV_NAME).exists())
            self.assertFalse((slow_dir / config.WORK_DIR_NAME).exists())
            # 解锁前登记仍挂起：已提交状态保持 running，查询依旧可读
            self.assertEqual(
                self.client.get(f"/batches/{slow['id']}").json()["status"], "running"
            )
        finally:
            # 6) 释放写锁
            self.release_write_lock(holder)

        # 7) 解锁后终态登记自动接续，各批次落到正确终态（不重跑脚本）
        ok_result = self.wait_for(ok["id"], timeout=10)
        self.assertEqual(ok_result["status"], "succeeded", ok_result["detail"])
        self.assertEqual(ok_result["exit_code"], 0)
        boom_result = self.wait_for(boom["id"], timeout=10)
        self.assertEqual(boom_result["status"], "failed")
        self.assertEqual(boom_result["exit_code"], 3)
        slow_result = self.wait_for(slow["id"], timeout=10)
        self.assertEqual(slow_result["status"], "timeout")
        self.assertIn("10", slow_result["detail"])

        # 8) 已发布结果未回退；超时/失败批次不提供残件下载
        self.assertEqual(
            self.client.get(f"/batches/{done['id']}").json()["status"], "succeeded"
        )
        self.assertEqual(
            self.client.get(f"/batches/{done['id']}/download").status_code, 200
        )
        self.assertEqual(
            self.client.get(f"/batches/{ok['id']}/download").status_code, 200
        )
        self.assertEqual(
            self.client.get(f"/batches/{boom['id']}/download").status_code, 409
        )
        self.assertEqual(
            self.client.get(f"/batches/{slow['id']}/download").status_code, 409
        )
        self.assert_no_leftover_processes()

    def test_15_queued_publish_losing_to_cancel_not_published(self) -> None:
        """竞态兜底：发布登记落库时若取消已先生效（条件更新未命中）——
        状态保持 cancelled 不被覆盖、残件清除、终态字段补齐、不可下载。
        """
        bid = uuid.uuid4().hex
        batch_dir = Path(config.BATCHES_ROOT) / bid
        work_dir = batch_dir / config.WORK_DIR_NAME
        work_dir.mkdir(parents=True)
        (batch_dir / config.OUTPUT_CSV_NAME).write_text(
            "hole_id,depth_m,grade_pct\nZK-1,1.0,2.0\n", encoding="utf-8"
        )
        now = db.utcnow()
        db.execute(
            "INSERT INTO batches (id, status, created_at, started_at, dir_path, "
            "input_path, script_path, work_dir, input_rows) VALUES (?,?,?,?,?,?,?,?,?)",
            (
                bid, config.STATUS_CANCELLED, now, now, str(batch_dir),
                str(batch_dir / "input.csv"), str(batch_dir / "script.py"),
                str(work_dir), 1,
            ),
        )
        # 模拟发布登记落库未命中（rowcount=0）：取消已先受理
        runner._on_terminal_applied(bid, batch_dir, config.STATUS_SUCCEEDED, 0)
        self.assertTrue(db.flush_durable(timeout=5), "补写终态字段的持久写未落库")

        row = db.query_one("SELECT * FROM batches WHERE id = ?", (bid,))
        self.assertEqual(row["status"], "cancelled")  # 已受理的取消不被回退
        self.assertIsNotNone(row["finished_at"])
        self.assertIn("取消", row["detail"])
        # 输出残件已清除，work 目录已删除
        self.assertFalse((batch_dir / config.OUTPUT_CSV_NAME).exists())
        self.assertFalse(work_dir.exists())
        # API 视角同样不可下载
        self.assertEqual(self.client.get(f"/batches/{bid}/download").status_code, 409)


if __name__ == "__main__":
    unittest.main(verbosity=2)
