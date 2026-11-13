#!/usr/bin/env python3
"""示例 3：慢任务脚本（仅标准库），用于演示 10 秒超时与主动取消。

脚本先睡眠 12 秒（超过平台 10 秒上限）再写出合法输出：
  - 不干预时批次在 10 秒时被整组终止，状态 timeout，残件清除；
  - 10 秒内调用取消接口则状态 cancelled。
"""

import csv
import sys
import time


def main(input_path: str, output_path: str) -> int:
    with open(input_path, newline="", encoding="utf-8-sig") as fh:
        data_rows = list(csv.reader(fh))[1:]

    for _ in range(60):  # 分片睡眠，便于信号快速打断
        time.sleep(0.2)

    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["hole_id", "depth_m", "grade_pct"])
        for hole_id, depth_raw, grade_raw in data_rows:
            writer.writerow(
                [hole_id, f"{float(depth_raw):.4f}", f"{float(grade_raw):.2f}"]
            )
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("用法: python slow_loop.py <输入路径> <输出路径>", file=sys.stderr)
        sys.exit(64)
    sys.exit(main(sys.argv[1], sys.argv[2]))
