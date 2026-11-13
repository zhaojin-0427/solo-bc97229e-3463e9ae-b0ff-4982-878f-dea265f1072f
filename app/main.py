"""FastAPI 路由：上传、查询、取消、下载。

状态机：running ──► succeeded / failed / timeout / cancelled
重启恢复：服务启动时所有遗留 running 批次标记为 interrupted，不可下载。
"""

from __future__ import annotations

import threading
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, HTTPException, UploadFile, status
from fastapi.responses import FileResponse
from pydantic import BaseModel

from . import config, db, runner, validation


class BatchOut(BaseModel):
    id: str
    status: str
    created_at: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    input_rows: int
    output_rows: Optional[int] = None
    exit_code: Optional[int] = None
    detail: Optional[str] = None
    download_url: Optional[str] = None


def _to_out(row: dict) -> BatchOut:
    download_url = f"/batches/{row['id']}/download" if row["status"] == config.STATUS_SUCCEEDED else None
    return BatchOut(
        id=row["id"],
        status=row["status"],
        created_at=row["created_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        input_rows=row["input_rows"],
        output_rows=row["output_rows"],
        exit_code=row["exit_code"],
        detail=row["detail"],
        download_url=download_url,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.connect()
    # 重启恢复：上次未跑完的批次一律标 interrupted，不可下载
    cur = db.execute(
        "UPDATE batches SET status = ?, finished_at = ?, detail = ? "
        "WHERE status = ?",
        (
            config.STATUS_INTERRUPTED,
            db.utcnow(),
            "服务重启时批次仍在执行，标记为中断，结果不可下载",
            config.STATUS_RUNNING,
        ),
    )
    if cur.rowcount:
        print(f"[startup] {cur.rowcount} 个未完成批次标记为 interrupted")
    yield
    # 优雅关闭：仍在跑的批次同样标记 interrupted（容器停止后进程组随之消失）
    db.execute(
        "UPDATE batches SET status = ?, finished_at = ?, detail = ? "
        "WHERE status = ?",
        (
            config.STATUS_INTERRUPTED,
            db.utcnow(),
            "服务关闭时批次仍在执行，标记为中断，结果不可下载",
            config.STATUS_RUNNING,
        ),
    )
    db.close()


app = FastAPI(
    title="钻孔 CSV 转换 API",
    description=(
        "上传钻孔 CSV（hole_id, depth_raw, grade_raw）与仅用标准库的 "
        "Python 转换脚本，后台执行并校验输出（hole_id, depth_m, grade_pct）。"
        "支持批次查询、取消与超时控制，结果持久化、重启可查。"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


@app.post(
    "/batches",
    response_model=BatchOut,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["batches"],
    summary="上传 CSV 与脚本，创建转换批次",
)
async def create_batch(
    csv_file: UploadFile = File(..., description="三列：hole_id,depth_raw,grade_raw"),
    script_file: UploadFile = File(..., description="仅用 Python 标准库的 .py 脚本"),
) -> BatchOut:
    csv_bytes = await csv_file.read(config.MAX_UPLOAD_BYTES + 1)
    if len(csv_bytes) > config.MAX_UPLOAD_BYTES:
        raise HTTPException(413, f"CSV 文件超过 {config.MAX_UPLOAD_BYTES} 字节上限")
    script_bytes = await script_file.read(config.MAX_UPLOAD_BYTES + 1)
    if len(script_bytes) > config.MAX_UPLOAD_BYTES:
        raise HTTPException(413, f"脚本文件超过 {config.MAX_UPLOAD_BYTES} 字节上限")

    if not csv_bytes:
        raise HTTPException(400, "CSV 文件为空")
    if not script_bytes:
        raise HTTPException(400, "脚本文件为空")
    if not (script_file.filename or "").lower().endswith(".py"):
        raise HTTPException(400, "脚本必须是 .py 文件")

    try:
        csv_text = csv_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise HTTPException(400, "CSV 必须为 UTF-8 编码") from None
    try:
        script_text = script_bytes.decode("utf-8")
    except UnicodeDecodeError:
        raise HTTPException(400, "脚本必须为 UTF-8 编码") from None

    try:
        checked = validation.validate_input_csv(csv_text)
    except validation.ValidationError as exc:
        raise HTTPException(422, f"输入 CSV 校验失败: {exc}") from None

    batch_id = uuid.uuid4().hex
    batch_dir = config.BATCHES_ROOT / batch_id
    work_dir = batch_dir / config.WORK_DIR_NAME
    work_dir.mkdir(parents=True, exist_ok=False)

    input_path = batch_dir / config.INPUT_CSV_NAME
    script_path = batch_dir / config.SCRIPT_NAME
    input_path.write_text(csv_text, encoding="utf-8")
    # 去掉可执行位，脚本一律以 python -I 显式解释执行
    script_path.write_text(script_text, encoding="utf-8")

    now = db.utcnow()
    db.execute(
        """
        INSERT INTO batches (id, status, created_at, started_at, dir_path,
                             input_path, script_path, work_dir, input_rows)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            batch_id,
            config.STATUS_RUNNING,
            now,
            now,
            str(batch_dir),
            str(input_path),
            str(script_path),
            str(work_dir),
            len(checked.rows),
        ),
    )

    thread = threading.Thread(
        target=runner.run_batch,
        args=(batch_id, checked.hole_ids),
        name=f"batch-{batch_id}",
        daemon=True,
    )
    thread.start()

    row = db.query_one("SELECT * FROM batches WHERE id = ?", (batch_id,))
    return _to_out(row)


@app.get("/batches", response_model=list[BatchOut], tags=["batches"], summary="列出全部批次")
def list_batches() -> list[BatchOut]:
    return [_to_out(r) for r in db.query_all("SELECT * FROM batches ORDER BY created_at DESC")]


@app.get("/batches/{batch_id}", response_model=BatchOut, tags=["batches"], summary="查询单个批次")
def get_batch(batch_id: str) -> BatchOut:
    row = db.query_one("SELECT * FROM batches WHERE id = ?", (batch_id,))
    if row is None:
        raise HTTPException(404, "批次不存在")
    return _to_out(row)


@app.post(
    "/batches/{batch_id}/cancel",
    response_model=BatchOut,
    tags=["batches"],
    summary="取消批次（仅 running 可取消，先到先得）",
)
def cancel_batch(batch_id: str) -> BatchOut:
    row = db.query_one("SELECT * FROM batches WHERE id = ?", (batch_id,))
    if row is None:
        raise HTTPException(404, "批次不存在")
    # 条件更新：取消先受理则状态翻为 cancelled，执行器轮询到后终止整组进程
    cur = db.execute(
        "UPDATE batches SET status = ? WHERE id = ? AND status = ?",
        (config.STATUS_CANCELLED, batch_id, config.STATUS_RUNNING),
    )
    if cur.rowcount == 0:
        # 发布（succeeded）或其它终态先生效 → 拒绝取消
        raise HTTPException(
            409,
            f"批次当前状态为 {row['status']}，已不可取消（终态先于取消生效）",
        )
    refreshed = db.query_one("SELECT * FROM batches WHERE id = ?", (batch_id,))
    return _to_out(refreshed)


@app.get(
    "/batches/{batch_id}/download",
    tags=["batches"],
    summary="下载转换结果（仅 succeeded 可下载）",
)
def download_batch(batch_id: str) -> FileResponse:
    row = db.query_one("SELECT * FROM batches WHERE id = ?", (batch_id,))
    if row is None:
        raise HTTPException(404, "批次不存在")
    if row["status"] != config.STATUS_SUCCEEDED or not row["output_path"]:
        raise HTTPException(
            409, f"批次状态为 {row['status']}，仅 succeeded 批次可下载"
        )
    output = Path(row["output_path"])
    if not output.is_file():
        # 持久化卷中的结果文件丢失，保守地拒绝下载
        raise HTTPException(410, "结果文件已不存在")
    return FileResponse(
        output,
        media_type="text/csv; charset=utf-8",
        filename=f"output_{batch_id}.csv",
    )


@app.get("/health", tags=["meta"], summary="健康检查")
def health() -> dict:
    return {"status": "ok"}
