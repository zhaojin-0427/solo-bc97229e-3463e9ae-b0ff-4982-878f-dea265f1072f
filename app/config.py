"""全局配置：全部可通过环境变量覆盖。"""

from __future__ import annotations

import os
from pathlib import Path

# 所有批次目录与 SQLite 数据库的根目录（容器内为 /data，挂载持久化卷）
DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/data")).resolve()
BATCHES_ROOT = DATA_ROOT / "batches"
DB_PATH = DATA_ROOT / "app.db"

# 单批次最长执行时间（秒），到时整组进程被杀掉并清理残件
BATCH_TIMEOUT_SECONDS = float(os.environ.get("BATCH_TIMEOUT_SECONDS", "10"))
# 单个上传文件大小上限
MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_BYTES", str(2 * 1024 * 1024)))
# 失败时回显 stderr 的最大字符数
MAX_DETAIL_CHARS = int(os.environ.get("MAX_DETAIL_CHARS", "2000"))

# 批次目录内的固定文件名
INPUT_CSV_NAME = "input.csv"
SCRIPT_NAME = "script.py"
WORK_DIR_NAME = "work"
OUTPUT_CSV_NAME = "output.csv"
RUN_LOG_NAME = "runner.log"
STDOUT_LOG_NAME = "stdout.log"
STDERR_LOG_NAME = "stderr.log"

# 批次状态
STATUS_RUNNING = "running"
STATUS_SUCCEEDED = "succeeded"
STATUS_FAILED = "failed"
STATUS_TIMEOUT = "timeout"
STATUS_CANCELLED = "cancelled"
STATUS_INTERRUPTED = "interrupted"

UNFINISHED_STATUSES = (STATUS_RUNNING,)
TERMINAL_STATUSES = (
    STATUS_SUCCEEDED,
    STATUS_FAILED,
    STATUS_TIMEOUT,
    STATUS_CANCELLED,
    STATUS_INTERRUPTED,
)
