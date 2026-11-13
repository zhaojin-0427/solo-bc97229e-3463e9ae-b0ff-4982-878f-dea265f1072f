"""SQLite 持久化层（仅标准库 sqlite3）。

连接模型
--------
* 写连接与读连接分离，库文件启用 WAL：
  - 读连接（query_one / query_all）：WAL 快照读。另一连接即使持续持有
    写锁，查询依然能读到最新已提交状态，绝不阻塞、绝不拖住 API；
  - 写连接：busy_timeout=0，遇锁立即报错而不是持锁等待，
    任何线程都不会因为等锁而卡住同进程的其他线程。
* 两类写操作：
  - execute：同步写（建批、取消、重启恢复），在有限预算内自动重试，
    预算耗尽抛 DatabaseBusyError（由 API 层映射为 503）；
  - execute_durable：持久写（批次终态登记），进入队列由后台登记线程
    按序应用，遇写锁自动退避重试直至落库——解锁后超时批次落 timeout、
    失败批次落 failed、校验通过批次落 succeeded；调用线程即刻返回，
    一个批次的登记永远拖不住其他批次的执行管控。

服务以单 uvicorn worker 运行；批次状态的关键翻转使用条件 UPDATE
（WHERE status = 前置状态），在真正落库时求值，保证取消与发布互斥。
"""

from __future__ import annotations

import queue
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Optional

from . import config

_SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id           TEXT PRIMARY KEY,
    status       TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    started_at   TEXT,
    finished_at  TEXT,
    dir_path     TEXT NOT NULL,
    input_path   TEXT NOT NULL,
    script_path  TEXT NOT NULL,
    work_dir     TEXT NOT NULL,
    output_path  TEXT,
    input_rows   INTEGER NOT NULL DEFAULT 0,
    output_rows  INTEGER,
    exit_code    INTEGER,
    detail       TEXT
);
CREATE INDEX IF NOT EXISTS idx_batches_created_at ON batches(created_at);
"""

# 同步写遇锁的重试总预算（秒）：覆盖瞬时锁竞争，又不让 API 长时间悬挂
SYNC_WRITE_BUSY_BUDGET = 2.5
# 写重试间隔（秒）
_RETRY_INTERVAL = 0.1
# 持久登记线程遇锁的退避间隔（秒）
_DURABLE_RETRY_INTERVAL = 0.2


class DatabaseBusyError(RuntimeError):
    """在限定预算内始终未拿到 SQLite 写锁（另一连接持续持锁）。"""


_lock_write = threading.RLock()  # 串行化写连接的单次尝试（尝试本身不阻塞）
_lock_read = threading.RLock()  # 串行化读连接（读不阻塞，锁仅为连接安全）
_wconn: Optional[sqlite3.Connection] = None
_rconn: Optional[sqlite3.Connection] = None


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_lock_error(exc: sqlite3.OperationalError) -> bool:
    return "locked" in str(exc).lower()


# ---------------------------------------------------------------------------
# 持久登记队列：遇锁自动重试的「必须落库」写
# ---------------------------------------------------------------------------


@dataclass
class _DurableWrite:
    sql: str
    params: tuple
    # 落库后的回调，参数为条件更新的 rowcount（0 表示前置状态未命中）
    on_applied: Optional[Callable[[int], None]] = None


_durable_queue: "queue.Queue[_DurableWrite]" = queue.Queue()
_writer_thread: Optional[threading.Thread] = None
_writer_stop: Optional[threading.Event] = None
_writer_mgmt_lock = threading.Lock()


def _rollback_quietly() -> None:
    """放弃遇锁失败的写事务，让下一次尝试从干净状态开始。"""
    conn = _wconn
    if conn is None:
        return
    try:
        with _lock_write:
            conn.rollback()
    except sqlite3.Error:
        pass


def _write_once(sql: str, params: tuple) -> int:
    """单次非阻塞写尝试；成功返回 rowcount，遇锁抛 OperationalError。"""
    conn = _wconn
    if conn is None:
        raise RuntimeError("数据库尚未连接")
    with _lock_write:
        cur = conn.execute(sql, params)
        conn.commit()
        return cur.rowcount


def _writer_loop(stop_event: threading.Event) -> None:
    """后台登记线程：按序应用持久写，遇写锁退避重试直至落库。"""
    pending: Optional[_DurableWrite] = None
    while not stop_event.is_set():
        if pending is None:
            try:
                pending = _durable_queue.get(timeout=0.1)
            except queue.Empty:
                continue
        try:
            rowcount = _write_once(pending.sql, pending.params)
        except sqlite3.OperationalError as exc:
            if _is_lock_error(exc):
                # 写锁被其它连接持有：退避后自动接续，绝不拖住调用线程
                _rollback_quietly()
                time.sleep(_DURABLE_RETRY_INTERVAL)
                continue
            print(f"[db] 持久写失败（非锁错误），丢弃: {exc!r} sql={pending.sql!r}")
            _durable_queue.task_done()
            pending = None
            continue
        except (sqlite3.Error, RuntimeError) as exc:
            print(f"[db] 持久写失败: {exc!r} sql={pending.sql!r}")
            _durable_queue.task_done()
            pending = None
            continue
        item, pending = pending, None
        if item.on_applied is not None:
            try:
                item.on_applied(rowcount)
            except Exception as exc:  # 回调异常不影响后续登记
                print(f"[db] 持久写回调异常: {exc!r}")
        _durable_queue.task_done()


def _ensure_writer() -> None:
    """登记线程异常退出时兜底重启；已关闭（stop 已置位）则不再重开。"""
    global _writer_thread
    with _writer_mgmt_lock:
        if _writer_thread is not None and _writer_thread.is_alive():
            return
        if _writer_stop is None or _writer_stop.is_set():
            return
        _writer_thread = threading.Thread(
            target=_writer_loop,
            args=(_writer_stop,),
            name="db-durable-writer",
            daemon=True,
        )
        _writer_thread.start()


def execute_durable(
    sql: str,
    params: Iterable[Any] = (),
    on_applied: Optional[Callable[[int], None]] = None,
) -> None:
    """登记一条「必须落库」的写：后台线程按序应用，遇写锁自动重试到成功。

    调用方即刻返回；on_applied 在真正落库后回调（参数为 rowcount）。
    """
    if _wconn is None:
        raise RuntimeError("数据库尚未连接")
    _durable_queue.put(_DurableWrite(sql, tuple(params), on_applied))
    _ensure_writer()


def flush_durable(timeout: float = 5.0) -> bool:
    """等待已受理的持久写全部落库（用于优雅关闭），返回是否清空。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _durable_queue.unfinished_tasks == 0:
            return True
        time.sleep(0.05)
    return _durable_queue.unfinished_tasks == 0


# ---------------------------------------------------------------------------
# 连接生命周期
# ---------------------------------------------------------------------------


def _init_schema(conn: sqlite3.Connection) -> None:
    """启用 WAL 并建表；写锁被占用时有限重试（启动路径）。"""
    deadline = time.monotonic() + 5.0
    while True:
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.executescript(_SCHEMA)
            conn.commit()
            return
        except sqlite3.OperationalError as exc:
            if not _is_lock_error(exc) or time.monotonic() >= deadline:
                raise
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
            time.sleep(_RETRY_INTERVAL)


def connect() -> None:
    """打开数据库（写连接 + 读连接）、初始化表结构并启动登记线程。"""
    global _wconn, _rconn, _writer_thread, _writer_stop
    config.DATA_ROOT.mkdir(parents=True, exist_ok=True)
    config.BATCHES_ROOT.mkdir(parents=True, exist_ok=True)

    with _lock_write:
        if _wconn is not None:
            _wconn.close()
        # timeout=0：遇锁立即报错，绝不在持锁等待中拖住其他线程
        _wconn = sqlite3.connect(config.DB_PATH, check_same_thread=False, timeout=0)
        _wconn.row_factory = sqlite3.Row
        _init_schema(_wconn)
    with _lock_read:
        if _rconn is not None:
            _rconn.close()
        _rconn = sqlite3.connect(config.DB_PATH, check_same_thread=False, timeout=0)
        _rconn.row_factory = sqlite3.Row

    if _writer_stop is not None:
        _writer_stop.set()
    _writer_stop = threading.Event()
    _writer_thread = None
    _ensure_writer()


def close() -> None:
    """尽力落库已受理的持久写，停止登记线程并关闭连接。"""
    global _wconn, _rconn
    try:
        flush_durable(timeout=5.0)
    except Exception:
        pass
    if _writer_stop is not None:
        _writer_stop.set()
    thread = _writer_thread
    if thread is not None and thread.is_alive():
        thread.join(timeout=5.0)
    with _lock_write:
        if _wconn is not None:
            _wconn.close()
            _wconn = None
    with _lock_read:
        if _rconn is not None:
            _rconn.close()
            _rconn = None


# ---------------------------------------------------------------------------
# 同步写与查询
# ---------------------------------------------------------------------------


def execute(
    sql: str,
    params: Iterable[Any] = (),
    busy_budget: float = SYNC_WRITE_BUSY_BUDGET,
) -> sqlite3.Cursor:
    """执行单条写 SQL 并提交，返回游标（可用 rowcount 判断条件更新是否命中）。

    另一连接持有写锁时在 busy_budget 预算内自动重试；预算耗尽抛
    DatabaseBusyError。重试发生在锁外，不会拖住读路径或其他线程。
    """
    if _wconn is None:
        raise RuntimeError("数据库尚未连接")
    deadline = time.monotonic() + busy_budget
    while True:
        try:
            with _lock_write:
                cur = _wconn.execute(sql, tuple(params))
                _wconn.commit()
                return cur
        except sqlite3.OperationalError as exc:
            if not _is_lock_error(exc):
                raise
            _rollback_quietly()
            if time.monotonic() >= deadline:
                raise DatabaseBusyError(
                    "数据库写锁被其它连接持续占用，写入暂时无法提交"
                ) from exc
            time.sleep(_RETRY_INTERVAL)


def query_one(sql: str, params: Iterable[Any] = ()) -> Optional[dict]:
    """只读查询（独立读连接 + WAL 快照）：外部写锁下仍能读已提交状态。"""
    if _rconn is None:
        raise RuntimeError("数据库尚未连接")
    with _lock_read:
        cur = _rconn.execute(sql, tuple(params))
        row = cur.fetchone()
    return dict(row) if row is not None else None


def query_all(sql: str, params: Iterable[Any] = ()) -> list[dict]:
    if _rconn is None:
        raise RuntimeError("数据库尚未连接")
    with _lock_read:
        cur = _rconn.execute(sql, tuple(params))
        rows = cur.fetchall()
    return [dict(r) for r in rows]
