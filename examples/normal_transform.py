#!/usr/bin/env python3
"""示例 1：正常转换脚本（仅使用 Python 标准库）。

用法: python normal_transform.py <输入CSV路径> <输出CSV路径>

约定:
  - 输入三列 hole_id,depth_raw,grade_raw，输出三列 hole_id,depth_m,grade_pct
  - depth_raw 按米解析，必须非负；grade_raw 为百分含量，必须在 0~100
  - 按原序保留全部孔号与重复行
退出码 0 表示脚本自身执行成功；是否可下载仍以平台输出校验为准。
"""

import csv
import sys


def convert(input_path: str, output_path: str) -> int:
    with open(input_path, newline="", encoding="utf-8-sig") as fh:
        rows = list(csv.reader(fh))

    if not rows:
        print("输入 CSV 为空", file=sys.stderr)
        return 1

    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["hole_id", "depth_m", "grade_pct"])
        for line_no, (hole_id, depth_raw, grade_raw) in enumerate(
            rows[1:], start=2
        ):
            depth_m = float(depth_raw)
            grade_pct = float(grade_raw)
            if depth_m < 0:
                raise ValueError(f"第 {line_no} 行深度为负: {depth_m}")
            if not 0.0 <= grade_pct <= 100.0:
                raise ValueError(f"第 {line_no} 行品位超出 0~100: {grade_pct}")
            writer.writerow([hole_id, f"{depth_m:.4f}", f"{grade_pct:.2f}"])
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("用法: python normal_transform.py <输入路径> <输出路径>", file=sys.stderr)
        sys.exit(64)
    sys.exit(convert(sys.argv[1], sys.argv[2]))
