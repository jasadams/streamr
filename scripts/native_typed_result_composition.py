#!/usr/bin/env python3
"""Prepare the generic scalar/typed-array composition manifest; never runs SQL."""
import argparse
import json
from pathlib import Path

import native_updating_top5_cdc as top


def prepare(directory):
    directory.mkdir(parents=True, exist_ok=False)
    source = top.events()
    live, states = {}, []
    for event in source:
        row = event["after"] if event["after"] is not None else event["before"]
        if event["after"] is None:
            del live[row["row_id"]]
        else:
            live[row["row_id"]] = event["after"]
        ranks = top.state(live)
        groups = {}
        for member in live.values():
            key = member["k"]
            groups.setdefault(key, dict(k=key, total=0,
                top_items=ranks.get(key, {}).get("top_items", [])))["total"] += 1
        states.append([groups[key] for key in sorted(groups)])
    (directory / "input.jsonl").write_text("".join(json.dumps(event) + "\n" for event in source))
    query = Path(__file__).with_name("native-typed-result-composition.sql").read_text()
    (directory / "query.sql").write_text(query)
    expected = dict(key_fields=["k"], prefix_states=states,
                    intermediate_values=dict(total=[None], top_items=[[]]),
                    checkpoint=states[top.CHECKPOINT - 1], final=states[-1])
    (directory / "expected.json").write_text(json.dumps(expected, indent=2) + "\n")
    manifest = dict(query="query.sql", input="input.jsonl", expected="expected.json",
                    checkpoint_input_rows=top.CHECKPOINT, max_capture_rows=128,
                    timeout_seconds=90, test_limits=dict(MAX_OPEN_DATABASES=4,
                    MAX_SNAPSHOTS=8, SCAN_PAGE_BYTES=33554432, QUEUED_WRITE_BYTES=67108864))
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(dict(status="prepared; runtime unverified", checkpoint=expected["checkpoint"],
                         final=expected["final"]), indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    prepare(parser.parse_args().directory.resolve())
