#!/usr/bin/env python3
"""示例 4：输出非法的脚本（仅标准库），退出码为 0 但输出无法通过校验。

演示“退出码 0 ≠ 可下载”：平台校验发现 grade_pct 超出 0~100、
depth_m 为负等问题时判 failed 并清除残件。
"""

import csv
import sys


def main(input_path: str, output_path: str) -> int:
    with open(input_path, newline="", encoding="utf-8-sig") as fh:
        data_rows = list(csv.reader(fh))[1:]

    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["hole_id", "depth_m", "grade_pct"])
        for index, (hole_id, depth_raw, grade_raw) in enumerate(data_rows):
            if index == 0:
                # 故意制造非法值：负深度 + 品位超过 100
                writer.writerow([hole_id, "-1.0000", "150.00"])
            else:
                writer.writerow(
                    [hole_id, f"{float(depth_raw):.4f}", f"{float(grade_raw):.2f}"]
                )
    return 0  # 脚本自身成功退出，但输出校验不会通过


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("用法: python invalid_output.py <输入路径> <输出路径>", file=sys.stderr)
        sys.exit(64)
    sys.exit(main(sys.argv[1], sys.argv[2]))
