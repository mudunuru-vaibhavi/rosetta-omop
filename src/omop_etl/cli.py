"""Command line: `omop-etl run` runs everything; each task can also run on its own.

    omop-etl run                       # whole pipeline, in dependency order
    omop-etl run --only data_quality   # one task (its inputs must already exist)
    omop-etl tasks                     # list tasks and their dependencies
    omop-etl dq                        # print the data-quality summary
"""

from __future__ import annotations

import argparse
import json
import sys

from .config import get_paths
from .pipeline import TASKS, FatalQualityError, ordered, run_all


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="omop-etl", description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--data-dir", help="data folder (default ./data or $OMOP_DATA_DIR)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="run the pipeline")
    r.add_argument("--only", nargs="+", choices=[t.name for t in TASKS])
    sub.add_parser("tasks", help="list tasks")
    sub.add_parser("dq", help="show data-quality results")
    args = ap.parse_args(argv)

    if args.cmd == "tasks":
        for t in ordered():
            print(f"{t.name:18s} <- {', '.join(t.upstream) or '-':40s} {t.doc}")
        return 0
    if args.cmd == "dq":
        data = json.loads((get_paths(args.data_dir).reports / "dq_results.json").read_text())
        for res in data["results"]:
            if res["status"] != "PASS" or res["flagged"]:
                print(f"{res['status']:5s} {res['check_name']:34s} {res['table']}.{res['field']}: "
                      f"{res['num_violated']}/{res['num_denominator']} ({res['pct_violated']}%)")
        return 0
    try:
        run_all(args.data_dir, args.only)
    except FatalQualityError as e:
        print(f"STOPPED: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
