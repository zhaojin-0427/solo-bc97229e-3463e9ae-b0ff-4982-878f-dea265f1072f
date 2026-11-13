"""端到端测试：启动真实 uvicorn 服务（随机端口）走 HTTP 调用。

覆盖：正常转换、父进程先退出/子进程持续写、10 秒超时、取消与发布竞态、
非法输出、非零退出、输入校验、批次隔离、重启恢复、进程组泄漏检查。
"""

from __future__ import annotations

import os
import shutil
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path

import httpx
import uvicorn

# 在导入应用前指定隔离的数据根目录
_TMP_ROOT = tempfile.mkdtemp(prefix="drill-api-test-")
os.environ["DATA_ROOT"] = _TMP_ROOT
os.environ.setdefault("BATCH_TIMEOUT_SECONDS", "10")

from app import config, db  # noqa: E402
from app.main import app  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
EXAMPLES = REPO_ROOT / "examples"

SAMPLE_CSV = (EXAMPLES / "sample_input.csv").read_bytes()
NORMAL_SCRIPT = (EXAMPLES / "normal_transform.py").read_bytes()
PARENT_EXIT_SCRIPT = (EXAMPLES / "parent_exits_child_writes.py").read_bytes()
SLOW_SCRIPT = (EXAMPLES / "slow_loop.py").read_bytes()
INVALID_SCRIPT = (EXAMPLES / "invalid_output.py").read_bytes()

EXIT_3_SCRIPT = b"import sys\nsys.exit(3)\n"


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


if __name__ == "__main__":
    unittest.main(verbosity=2)
