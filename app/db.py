"""SQLite 持久化层（仅标准库 sqlite3）。

写操作串行化于一把进程内锁；服务以单 uvicorn worker 运行，
批次状态的关键翻转使用条件 UPDATE 保证原子性。
"""

from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

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

_lock = threading.RLock()
_conn: Optional[sqlite3.Connection] = None


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect() -> None:
    """打开数据库并初始化表结构。"""
    global _conn
    config.DATA_ROOT.mkdir(parents=True, exist_ok=True)
    config.BATCHES_ROOT.mkdir(parents=True, exist_ok=True)
    _conn = sqlite3.connect(config.DB_PATH, check_same_thread=False)
    _conn.row_factory = sqlite3.Row
    with _lock:
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA synchronous=NORMAL")
        _conn.executescript(_SCHEMA)
        _conn.commit()


def close() -> None:
    global _conn
    with _lock:
        if _conn is not None:
            _conn.close()
            _conn = None


def execute(sql: str, params: Iterable[Any] = ()) -> sqlite3.Cursor:
    """执行单条写 SQL 并提交，返回游标（可用 rowcount 判断条件更新是否命中）。"""
    if _conn is None:
        raise RuntimeError("数据库尚未连接")
    with _lock:
        cur = _conn.execute(sql, tuple(params))
        _conn.commit()
        return cur


def query_one(sql: str, params: Iterable[Any] = ()) -> Optional[dict]:
    if _conn is None:
        raise RuntimeError("数据库尚未连接")
    with _lock:
        cur = _conn.execute(sql, tuple(params))
        row = cur.fetchone()
    return dict(row) if row is not None else None


def query_all(sql: str, params: Iterable[Any] = ()) -> list[dict]:
    if _conn is None:
        raise RuntimeError("数据库尚未连接")
    with _lock:
        cur = _conn.execute(sql, tuple(params))
        rows = cur.fetchall()
    return [dict(r) for r in rows]
