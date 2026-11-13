"""输入 / 输出 CSV 校验。

输入要求：表头严格为 hole_id,depth_raw,grade_raw，字段非空。
输出要求：表头严格为 hole_id,depth_m,grade_pct；
深度非负、品位 0~100；行数与输入一致；
hole_id 序列（含重复行、原顺序、原值）与输入完全一致。
"""

from __future__ import annotations

import csv
from dataclasses import dataclass

INPUT_HEADER = ["hole_id", "depth_raw", "grade_raw"]
OUTPUT_HEADER = ["hole_id", "depth_m", "grade_pct"]


class ValidationError(ValueError):
    """CSV 内容不符合约定。"""


@dataclass
class InputCheck:
    rows: list[dict[str, str]]
    hole_ids: list[str]


def _read_rows(text: str) -> list[list[str]]:
    return [row for row in csv.reader(text.splitlines())]


def validate_input_csv(text: str) -> InputCheck:
    rows = _read_rows(text)
    if not rows:
        raise ValidationError("CSV 为空，缺少表头")
    header = [h.strip() for h in rows[0]]
    if header != INPUT_HEADER:
        raise ValidationError(f"输入表头必须为 {INPUT_HEADER}，实际为 {header}")

    parsed: list[dict[str, str]] = []
    hole_ids: list[str] = []
    for line_no, raw in enumerate(rows[1:], start=2):
        if not raw or all(cell.strip() == "" for cell in raw):
            raise ValidationError(f"第 {line_no} 行为空行")
        if len(raw) != 3:
            raise ValidationError(f"第 {line_no} 行列数为 {len(raw)}，应为 3")
        hole_id, depth_raw, grade_raw = (cell.strip() for cell in raw)
        if not hole_id:
            raise ValidationError(f"第 {line_no} 行 hole_id 为空")
        if not depth_raw:
            raise ValidationError(f"第 {line_no} 行 depth_raw 为空")
        if not grade_raw:
            raise ValidationError(f"第 {line_no} 行 grade_raw 为空")
        parsed.append(
            {"hole_id": hole_id, "depth_raw": depth_raw, "grade_raw": grade_raw}
        )
        hole_ids.append(hole_id)

    if not parsed:
        raise ValidationError("CSV 没有数据行")
    return InputCheck(rows=parsed, hole_ids=hole_ids)


def validate_output_csv(text: str, expected_hole_ids: list[str]) -> int:
    """校验输出 CSV，返回输出数据行数。"""
    rows = _read_rows(text)
    if not rows:
        raise ValidationError("输出 CSV 为空，缺少表头")
    header = [h.strip() for h in rows[0]]
    if header != OUTPUT_HEADER:
        raise ValidationError(f"输出表头必须为 {OUTPUT_HEADER}，实际为 {header}")

    out_ids: list[str] = []
    for line_no, raw in enumerate(rows[1:], start=2):
        if not raw or all(cell.strip() == "" for cell in raw):
            raise ValidationError(f"输出第 {line_no} 行为空行")
        if len(raw) != 3:
            raise ValidationError(f"输出第 {line_no} 行列数为 {len(raw)}，应为 3")
        hole_id, depth_s, grade_s = (cell.strip() for cell in raw)

        try:
            depth = float(depth_s)
        except ValueError:
            raise ValidationError(
                f"输出第 {line_no} 行 depth_m 不是合法数字: {depth_s!r}"
            ) from None
        try:
            grade = float(grade_s)
        except ValueError:
            raise ValidationError(
                f"输出第 {line_no} 行 grade_pct 不是合法数字: {grade_s!r}"
            ) from None

        if depth < 0:
            raise ValidationError(f"输出第 {line_no} 行 depth_m 为负数: {depth}")
        if not (0.0 <= grade <= 100.0):
            raise ValidationError(
                f"输出第 {line_no} 行 grade_pct 超出 0~100 范围: {grade}"
            )
        out_ids.append(hole_id)

    if len(out_ids) != len(expected_hole_ids):
        raise ValidationError(
            f"输出行数 {len(out_ids)} 与输入行数 {len(expected_hole_ids)} 不一致"
        )
    if out_ids != expected_hole_ids:
        raise ValidationError(
            "输出 hole_id 序列与输入不一致：必须按原序保留孔号与重复行"
        )
    return len(out_ids)
