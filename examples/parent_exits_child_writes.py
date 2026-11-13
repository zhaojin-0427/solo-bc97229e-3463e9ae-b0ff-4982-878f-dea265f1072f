#!/usr/bin/env python3
"""示例 2：父进程先退出、子进程持续写文件（仅使用 Python 标准库）。

用法: python parent_exits_child_writes.py <输入CSV路径> <输出CSV路径>

行为:
  - 父脚本派生一个【同进程组】的普通子进程后，立即以退出码 0 退出；
  - 子进程继续存活，边等待边向输出文件写数据，默认持续 8 秒。

平台正确行为:
  - 不得因父进程退出码为 0 就提前发布：执行器会发现进程组内仍有
    子进程存活，宽限 1 秒后整组 SIGKILL，批次判 failed、残件清除。
  - 设置环境变量 BATCH_DEMO_SECONDS=30（在 docker-compose 中给服务
    加该变量）可让子进程存活超过 10 秒，用于演示超时终止。
"""

import csv
import os
import subprocess
import sys
import time

CHILD_ENV = "_DRILL_DEMO_CHILD"


def run_child(input_path: str, output_path: str) -> int:
    with open(input_path, newline="", encoding="utf-8-sig") as fh:
        data_rows = list(csv.reader(fh))[1:]

    duration = float(os.environ.get("BATCH_DEMO_SECONDS", "8"))
    end_at = time.monotonic() + duration

    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["hole_id", "depth_m", "grade_pct"])
        fh.flush()
        for hole_id, depth_raw, grade_raw in data_rows:
            remaining = end_at - time.monotonic()
            if remaining > 0:
                time.sleep(min(1.0, remaining))
            writer.writerow(
                [hole_id, f"{float(depth_raw):.4f}", f"{float(grade_raw):.2f}"]
            )
            fh.flush()  # 持续写文件：若平台误发布，下载到的将是半成品
    return 0


def main() -> int:
    if len(sys.argv) != 3:
        print("用法: python parent_exits_child_writes.py <输入路径> <输出路径>",
              file=sys.stderr)
        return 64

    input_path, output_path = sys.argv[1], sys.argv[2]

    if os.environ.get(CHILD_ENV) == "1":
        return run_child(input_path, output_path)

    # 父进程：在同一进程组内派生子进程（不使用 start_new_session），
    # 不 wait，直接退出并返回 0。
    child_env = os.environ.copy()
    child_env[CHILD_ENV] = "1"
    child = subprocess.Popen(
        [sys.executable, "-I", os.path.abspath(__file__), input_path, output_path],
        env=child_env,
    )
    print(f"父进程: 已派生子进程 pid={child.pid}，父进程立即退出 (code=0)", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
