#!/usr/bin/env python3
"""Qualify existing SQL uuid() as one bounded value in a fused event owner.

The caller supplies a fresh arroyo-sql-testing binary. The generic fixture
looks up a prior row, runs two dependent MERGEs, and captures the first and
final branches. It checks UUID consistency rather than predicting randomness;
an uncheckpointed replay is allowed to generate a different candidate.
"""

import argparse
import json
import os
from pathlib import Path
import subprocess
import uuid


EVENTS = [(1, 11), (2, 22), (3, 11), (4, 22)]
PREFIX = 2
FINAL_FIELDS = {
    "seq", "item_id", "candidate", "prior", "inventory_candidate",
    "inventory_action", "observed_candidate", "observed_action",
}
MID_FIELDS = {
    "seq", "item_id", "candidate", "prior", "inventory_candidate",
    "inventory_action",
}


def write_fixture(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "input.jsonl").write_text(
        "".join(json.dumps({"seq": seq, "item_id": item_id}) + "\n"
                for seq, item_id in EVENTS)
    )
    (directory / "query.sql").write_text(f"""
CREATE TABLE mutation_input (seq BIGINT NOT NULL, item_id BIGINT NOT NULL)
WITH (connector = 'single_file', path = '{directory}/input.jsonl', format = 'json',
      type = 'source', wait_for_control = 'true');
CREATE TABLE mutation_mid (seq BIGINT, item_id BIGINT, candidate TEXT,
  prior TEXT, inventory_candidate TEXT, inventory_action TEXT)
WITH (connector = 'single_file', path = '{directory}/mid.jsonl', format = 'json', type = 'sink');
CREATE TABLE mutation_output (seq BIGINT, item_id BIGINT, candidate TEXT,
  prior TEXT, inventory_candidate TEXT, inventory_action TEXT,
  observed_candidate TEXT, observed_action TEXT)
WITH (connector = 'single_file', path = '{directory}/output.jsonl', format = 'json', type = 'sink');
CREATE TABLE mutation_mirror (seq BIGINT, item_id BIGINT, candidate TEXT,
  prior TEXT, inventory_candidate TEXT, inventory_action TEXT,
  observed_candidate TEXT, observed_action TEXT)
WITH (connector = 'single_file', path = '{directory}/mirror.jsonl', format = 'json', type = 'sink');
CREATE STATE TABLE inventory (item_id BIGINT PRIMARY KEY, candidate TEXT NOT NULL)
PARTITION BY item_id;
CREATE STATE TABLE observed (item_id BIGINT PRIMARY KEY, candidate TEXT NOT NULL)
PARTITION BY item_id;
CREATE VIEW generated AS SELECT seq, item_id, uuid() AS candidate FROM mutation_input;
CREATE VIEW before_write AS SELECT g.seq, g.item_id, g.candidate,
  i.candidate AS prior FROM generated AS g LEFT JOIN inventory AS i
  ON i.item_id = g.item_id;
CREATE VIEW first_write AS MERGE INTO inventory AS target USING before_write AS source
ON target.item_id = source.item_id
WHEN NOT MATCHED THEN INSERT (item_id, candidate)
VALUES (source.item_id, source.candidate)
RETURNING source AS source, old AS old, new AS new, action AS action;
CREATE VIEW forwarded AS SELECT r.source.seq AS seq, r.source.item_id AS item_id,
  r.source.candidate AS candidate, r.source.prior AS prior,
  r.new.candidate AS inventory_candidate, r.action AS inventory_action
FROM first_write AS r;
CREATE VIEW second_write AS MERGE INTO observed AS target USING forwarded AS source
ON target.item_id = source.item_id
WHEN NOT MATCHED THEN INSERT (item_id, candidate)
VALUES (source.item_id, source.candidate)
RETURNING source AS source, old AS old, new AS new, action AS action;
CREATE VIEW checked AS SELECT r.source.seq AS seq, r.source.item_id AS item_id,
  r.source.candidate AS candidate, r.source.prior AS prior,
  r.source.inventory_candidate AS inventory_candidate,
  r.source.inventory_action AS inventory_action,
  r.new.candidate AS observed_candidate, r.action AS observed_action
FROM second_write AS r;
INSERT INTO mutation_mid SELECT seq, item_id, candidate, prior,
  inventory_candidate, inventory_action FROM forwarded;
INSERT INTO mutation_output SELECT seq, item_id, candidate, prior,
  inventory_candidate, inventory_action, observed_candidate, observed_action FROM checked;
INSERT INTO mutation_mirror SELECT seq, item_id, candidate, prior,
  inventory_candidate, inventory_action, observed_candidate, observed_action FROM checked;
""")


def read_rows(path: Path, count: int, fields: set[str]) -> list[dict]:
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert len(rows) == count, (path, len(rows), count)
    for row in rows:
        assert isinstance(row, dict) and set(row) == fields, (path, row)
    return rows


def check_uuid(value: str) -> None:
    parsed = uuid.UUID(value)
    assert parsed.version == 4 and str(parsed) == value, value


def check_output(rows: list[dict], mids: list[dict] | None = None) -> None:
    assert [(row["seq"], row["item_id"]) for row in rows] == EVENTS, rows
    if mids is not None:
        assert [{key: row[key] for key in MID_FIELDS} for row in rows] == mids, (rows, mids)
    bindings = {}
    for row in rows:
        check_uuid(row["candidate"])
        item_id = row["item_id"]
        if item_id not in bindings:
            assert row["prior"] is None, row
            assert row["inventory_action"] == "insert", row
            assert row["observed_action"] == "insert", row
            assert row["inventory_candidate"] == row["candidate"], row
            assert row["observed_candidate"] == row["candidate"], row
            bindings[item_id] = row["candidate"]
        else:
            retained = bindings[item_id]
            assert row["prior"] == retained, row
            assert row["inventory_action"] == "none", row
            assert row["observed_action"] == "none", row
            assert row["inventory_candidate"] == retained, row
            assert row["observed_candidate"] == retained, row
    assert set(bindings) == {11, 22}, bindings


def run_case(binary: Path, directory: Path, backend: str, batch: int, mode: str) -> None:
    write_fixture(directory)
    for name in ("output.jsonl", "output.initial.jsonl", "mid.jsonl",
                 "mirror.jsonl", "capture.log"):
        (directory / name).unlink(missing_ok=True)
    env = dict(os.environ)
    env.pop("STREAMR_TEST_NATIVE_WINDOWS", None)
    env.pop("STREAMR_TEST_NATIVE_AGGREGATES", None)
    env.update(
        STREAMR_TEST_TYPED_SQL="1",
        STREAMR_TEST_EXECUTION_BYTES="16777216",
        STREAMR_TEST_BACKEND=backend,
        STREAMR_TEST_SOURCE_BATCH_ROWS=str(batch),
        STREAMR_TEST_CHECKPOINT_MODE=mode,
        STREAMR_CAPTURE_QUERY=str(directory / "query.sql"),
        STREAMR_CAPTURE_OUTPUT=str(directory / "output.jsonl"),
        STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT=str(PREFIX),
        STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS=str(len(EVENTS)),
        STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS=str(PREFIX),
        STREAMR_CAPTURE_EXPECTED_ROWS=str(len(EVENTS)),
        STREAMR_CAPTURE_CHECKPOINT_EPOCH="1",
    )
    log = directory / "capture.log"
    with log.open("w") as output:
        result = subprocess.run(
            [str(binary), "external_sql_checkpoint_capture", "--ignored",
             "--test-threads=1", "--nocapture"],
            env=env, stdout=output, stderr=subprocess.STDOUT,
        )
    if result.returncode or "1 passed" not in log.read_text():
        raise RuntimeError(f"capture failed: {log}")
    initial = read_rows(directory / "output.initial.jsonl", len(EVENTS), FINAL_FIELDS)
    recovered = read_rows(directory / "output.jsonl", len(EVENTS), FINAL_FIELDS)
    mirror = read_rows(directory / "mirror.jsonl", len(EVENTS), FINAL_FIELDS)
    mids = read_rows(directory / "mid.jsonl", len(EVENTS), MID_FIELDS)
    assert recovered == mirror, (recovered, mirror)
    check_output(initial)
    # The harness first runs an uninterrupted job, then starts a separate job
    # for checkpoint/recovery. Their random candidates must not be compared.
    # Rows 3 and 4 independently prove that the restored bindings for *both*
    # keys match rows 1 and 2 in the same recovered execution.
    check_output(recovered, mids)
    print(f"PASS {directory.name}: same UUID across writes/captures and checkpoint", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--binary", type=Path, help="fresh arroyo-sql-testing binary")
    args = parser.parse_args()
    directory = args.directory.resolve()
    if args.binary is None:
        write_fixture(directory)
        print(f"Prepared generic UUID fixture: {directory}")
        return
    binary = args.binary.resolve(strict=True)
    for backend in ("memory", "rocksdb"):
        for batch in (1, 8):
            for mode in ("controller", "leader"):
                run_case(binary, directory / f"{backend}-{batch}-{mode}", backend, batch, mode)


if __name__ == "__main__":
    main()
