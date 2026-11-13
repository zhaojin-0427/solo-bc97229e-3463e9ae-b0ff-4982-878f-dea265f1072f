# 钻孔 CSV 转换 API

矿山数据工程师接收承包商随批提供的 Python 转换脚本，本服务将钻孔 CSV 转为入库记录：
后台执行承包商脚本、严格校验输出、控制超时与取消、持久化批次结果，并提供可演示的
交互式 API 文档（Swagger UI）。

## 功能约定

- **输入**：CSV 三列 `hole_id,depth_raw,grade_raw` + 仅使用 Python 标准库的 `.py` 脚本；
  脚本以「输入文件路径、输出文件路径」两个参数运行。
- **输出**：CSV 三列 `hole_id,depth_m,grade_pct`；按原序保留孔号与重复行，
  `depth_m >= 0`，`0 <= grade_pct <= 100`，行数与输入一致。
- **每批独立目录**：`/data/batches/<batch_id>/`，后台线程 + 独立进程组执行，批次互不影响。
- **可下载的三个必要条件（缺一不可）**：
  1. 主脚本退出码为 `0`；
  2. 进程组内全部普通子进程均已退出（父进程先退出、子进程仍在写文件时**绝不提前发布**）；
  3. 输出文件通过完整校验。
- **超时**：运行满 10 秒即对整组进程发 `SIGKILL`（含全部子进程），清除残件，状态置 `timeout`。
- **取消**：取消先受理则批次不得发布；发布先生效则取消返回 `409`。
  取消同样终止整组进程并清除残件，状态置 `cancelled`。
- **故障隔离**：任一批次超时/取消/失败不影响其他批次执行与查询。
- **重启恢复**：SQLite 与输出文件位于持久化卷；重启后已完成结果保留可下载，
  未完成批次标记 `interrupted` 且不可下载。

批次状态机：

```
running ──► succeeded（可下载）
         ├► failed（退出码非 0 / 子进程未退出 / 输出缺失或校验失败）
         ├► timeout（满 10 秒）
         └► cancelled（取消先受理）
重启时仍为 running ──► interrupted（不可下载）
```

## 目录结构

```
.
├── app/                    # FastAPI 应用
│   ├── config.py           # 环境变量配置与常量
│   ├── db.py               # SQLite 持久化（标准库 sqlite3）
│   ├── validation.py       # 输入/输出 CSV 校验
│   ├── runner.py           # 批次执行器：进程组、超时、取消、原子发布
│   └── main.py             # 路由：上传/查询/取消/下载 + 生命周期恢复
├── examples/               # 承包商脚本示例与样例数据
│   ├── sample_input.csv
│   ├── normal_transform.py            # 正常转换
│   ├── parent_exits_child_writes.py   # 父进程先退出、子进程持续写文件
│   ├── slow_loop.py                   # 睡眠 12 秒，演示超时/取消
│   └── invalid_output.py              # 退出码 0 但输出非法
├── tests/                  # 单元测试与端到端测试（含进程泄漏检测）
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## 快速启动（Docker Compose）

```bash
docker compose up --build
```

- API 服务：http://localhost:8000
- 交互式文档（Swagger UI）：http://localhost:8000/docs
- OpenAPI 描述：http://localhost:8000/openapi.json
- 健康检查：http://localhost:8000/health

数据保存在 Docker 命名卷 `drill-data`（容器内 `/data`，含 `app.db` 与各批次目录），
`docker compose down` 后数据仍在；`docker compose down -v` 才会清除。

## 在 Swagger UI 中演示

打开 http://localhost:8000/docs ，按以下顺序操作（各接口均点 **Try it out**）：

1. **POST `/batches`**：`csv_file` 选择 `examples/sample_input.csv`，
   `script_file` 选择 `examples/normal_transform.py`，Execute → 返回 `202` 与批次 `id`。
2. **GET `/batches/{batch_id}`**：填入 id 查询，状态由 `running` 变为 `succeeded`，
   响应中带 `download_url`。
3. **GET `/batches/{batch_id}/download`**：Execute 下载转换后的 CSV。
4. 再上传 `slow_loop.py`：立即调用 **POST `/batches/{batch_id}/cancel`** 可演示取消；
   不取消则约 10 秒后状态变为 `timeout`。
5. 上传 `parent_exits_child_writes.py`：父进程秒退返回 0，但因子进程仍存活，
   批次判 `failed` 且不能下载（响应 `detail` 说明原因）。
6. **GET `/batches`** 查看全部批次；对已 `succeeded` 的批次调用 cancel 会得到 `409`。

### 等价 curl

```bash
# 创建批次
curl -X POST http://localhost:8000/batches \
  -F "csv_file=@examples/sample_input.csv;type=text/csv" \
  -F "script_file=@examples/normal_transform.py;type=text/x-python"

# 查询 / 列表 / 取消 / 下载
curl http://localhost:8000/batches/<id>
curl http://localhost:8000/batches
curl -X POST http://localhost:8000/batches/<id>/cancel
curl -OJ http://localhost:8000/batches/<id>/download
```

## 关键设计说明

- **进程组整组管控**：脚本以 `start_new_session=True` 启动在独立会话/进程组中，
  承包商派生的普通子进程同组。执行器轮询 `/proc/<pid>/stat` 的 `pgrp` 确认组内
  是否还有存活进程；超时、取消、子进程赖场时 `killpg(SIGKILL)` 一锅端，并回收僵尸，
  杜绝孤儿子进程继续写输出文件。
- **发布原子性**：成功路径在「退出码 0 + 组内无存活进程 + 输出校验通过」后，
  用条件 `UPDATE ... WHERE status='running'` 原子翻转终态；取消用同样的条件更新抢占，
  两者先到先得，不可能出现「已取消又可下载」或「已发布又被取消」。
- **残件清除**：非成功批次的 `output.csv` 与 `work/` 在终态写入前删除，
  批次目录保留输入、脚本与 `runner.log/stdout.log/stderr.log` 便于审计。
- **不可信脚本防护**：脚本与数据放在独立目录；以 `python -I`（隔离模式）+
  `PYTHONSAFEPATH=1` 执行，解释器不会把脚本目录、当前目录加入 `sys.path`，
  防止上传目录中 planted 的 `json.py` 等同名模块被导入；容器以非 root 用户运行。
- **单 worker 运行**：进程与线程状态在进程内管理，Compose/启动命令固定
  `--workers 1`；持久化状态全部落 SQLite，重启可恢复。

## 本地测试

```bash
pip install -r requirements.txt httpx
PYTHONPATH=. python -m unittest discover -s tests -v
```

测试包含 13 个端到端用例（真实 uvicorn + HTTP 调用，含重启恢复）与 13 个校验单元测试，
并在结束时扫描 `/proc` 断言没有任何残留子进程。
