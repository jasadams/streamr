"""Small pure-Python output-oracle checks; no SQL process or large fixture."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("test-native-window-capacity.py")
SPEC = importlib.util.spec_from_file_location("window_capacity", SCRIPT)
capacity = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(capacity)


class CapacityOracleTests(unittest.TestCase):
    rows = 3
    payload_bytes = 16

    def output(self, values, case):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "output.jsonl"
            path.write_text("".join(json.dumps(row) + "\n" for row in values))
            return capacity.check_output(path, case, self.rows, self.payload_bytes)

    def item_rows(self, hop):
        starts = [capacity.BASE]
        width = 2
        if hop:
            starts.insert(0, capacity.BASE - capacity.timedelta(seconds=2))
            width = 4
        return [dict(item_id=item, start=start.isoformat(),
                     end=(start + capacity.timedelta(seconds=width)).isoformat(),
                     n=1, first_payload=capacity.payload_for(item, self.payload_bytes))
                for item in range(self.rows) for start in starts]

    def test_tumble_and_session_remain_valid(self):
        self.assertEqual(self.output(self.item_rows(False), "tumble"), self.rows)
        session = [dict(segment="hot", start=capacity.BASE.isoformat(),
                        end=(capacity.BASE + capacity.timedelta(seconds=10)).isoformat(),
                        n=self.rows,
                        first_payload=capacity.payload_for(0, self.payload_bytes))]
        self.assertEqual(self.output(session, "session"), 1)

    def test_hop_requires_both_exact_windows_per_item(self):
        values = self.item_rows(True)
        self.assertEqual(self.output(list(reversed(values)), "hop"), 2 * self.rows)
        with self.assertRaises(AssertionError):
            self.output(values[:-1], "hop")
        with self.assertRaises(AssertionError):
            self.output(values + [values[0]], "hop")

    def test_hop_rejects_wrong_window_payload_and_count_type(self):
        for field, wrong in (
            ("start", (capacity.BASE + capacity.timedelta(seconds=2)).isoformat()),
            ("first_payload", "wrong"),
            ("n", True),
        ):
            values = self.item_rows(True)
            values[0][field] = wrong
            with self.subTest(field=field), self.assertRaises(AssertionError):
                self.output(values, "hop")


if __name__ == "__main__":
    unittest.main()
