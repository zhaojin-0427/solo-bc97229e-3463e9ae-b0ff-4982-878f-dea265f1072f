"""validation 模块的纯单元测试。"""

import unittest

from app.validation import (
    ValidationError,
    validate_input_csv,
    validate_output_csv,
)


class InputValidationTests(unittest.TestCase):
    def test_ok_with_duplicates(self) -> None:
        text = "hole_id,depth_raw,grade_raw\nA,1,2\nB,3,4\nA,1,2\n"
        checked = validate_input_csv(text)
        self.assertEqual(checked.hole_ids, ["A", "B", "A"])
        self.assertEqual(len(checked.rows), 3)

    def test_bad_header(self) -> None:
        with self.assertRaises(ValidationError):
            validate_input_csv("hole_id,depth,grade\nA,1,2\n")

    def test_empty_cell(self) -> None:
        with self.assertRaises(ValidationError):
            validate_input_csv("hole_id,depth_raw,grade_raw\nA,,2\n")

    def test_no_data_rows(self) -> None:
        with self.assertRaises(ValidationError):
            validate_input_csv("hole_id,depth_raw,grade_raw\n")

    def test_wrong_column_count(self) -> None:
        with self.assertRaises(ValidationError):
            validate_input_csv("hole_id,depth_raw,grade_raw\nA,1\n")


class OutputValidationTests(unittest.TestCase):
    IDS = ["A", "B", "A"]

    def test_ok(self) -> None:
        text = "hole_id,depth_m,grade_pct\nA,0.0,0\nB,12.5,1.32\nA,9,100\n"
        self.assertEqual(validate_output_csv(text, self.IDS), 3)

    def test_negative_depth(self) -> None:
        text = "hole_id,depth_m,grade_pct\nA,-0.1,1\nB,1,2\nA,9,3\n"
        with self.assertRaises(ValidationError):
            validate_output_csv(text, self.IDS)

    def test_grade_over_100(self) -> None:
        text = "hole_id,depth_m,grade_pct\nA,1,100.01\nB,1,2\nA,9,3\n"
        with self.assertRaises(ValidationError):
            validate_output_csv(text, self.IDS)

    def test_grade_negative(self) -> None:
        text = "hole_id,depth_m,grade_pct\nA,1,-0.01\nB,1,2\nA,9,3\n"
        with self.assertRaises(ValidationError):
            validate_output_csv(text, self.IDS)

    def test_non_numeric(self) -> None:
        text = "hole_id,depth_m,grade_pct\nA,x,1\nB,1,2\nA,9,3\n"
        with self.assertRaises(ValidationError):
            validate_output_csv(text, self.IDS)

    def test_row_count_mismatch(self) -> None:
        text = "hole_id,depth_m,grade_pct\nA,1,1\nB,1,2\n"
        with self.assertRaises(ValidationError):
            validate_output_csv(text, self.IDS)

    def test_hole_id_order_mismatch(self) -> None:
        text = "hole_id,depth_m,grade_pct\nA,1,1\nA,1,2\nB,9,3\n"
        with self.assertRaises(ValidationError):
            validate_output_csv(text, self.IDS)

    def test_bad_header(self) -> None:
        with self.assertRaises(ValidationError):
            validate_output_csv("hole_id,depth,grade\n", self.IDS)


if __name__ == "__main__":
    unittest.main(verbosity=2)
