#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the full pipeline from datasets to both databases: "
            "generate distinct CSVs, generate lookup seeds, create SQL schema, generate SQL DML, load PostgreSQL, "
            "generate NoSQL JSON docs, load MongoDB."
        )
    )
    parser.add_argument(
        "--n",
        type=int,
        required=True,
        help="Number of anime IDs to sample for SQL DML generation.",
    )
    parser.add_argument(
        "--user-ids",
        required=False,
        help=(
            "Optional comma-separated app_user IDs for NoSQL document generation. "
            "If omitted, IDs are auto-derived from dml/seeds/021_app_user_seed.sql."
        ),
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional random seed for SQL DML generation.",
    )
    parser.add_argument(
        "--sql-connection-string",
        default=None,
        help="Optional PostgreSQL connection string override.",
    )
    parser.add_argument(
        "--nosql-connection-string",
        default=None,
        help="Optional MongoDB connection string override.",
    )
    parser.add_argument(
        "--nosql-clear",
        action="store_true",
        help="Clear Mongo users/ratings collections before insert.",
    )
    parser.add_argument(
        "--nosql-batch-size",
        type=int,
        default=1000,
        help="Mongo insert batch size for run-nosql.py (default: 1000).",
    )
    return parser.parse_args()


def run_step(step_number: int, total_steps: int, title: str, command: list[str]) -> None:
    print(f"\n[{step_number}/{total_steps}] {title}")
    print("$ " + " ".join(command))
    try:
        subprocess.run(command, cwd=ROOT, check=True)
    except subprocess.CalledProcessError as exc:
        raise SystemExit(f"Pipeline failed at step {step_number}: {title} (exit code {exc.returncode})") from exc


def parse_user_ids_from_app_user_seed(seed_path: Path) -> list[int]:
    if not seed_path.exists():
        raise SystemExit(f"Expected seed file not found: {seed_path}")

    content = seed_path.read_text(encoding="utf-8")
    ids = [int(match.group(1)) for match in re.finditer(r"\(\s*(\d+)\s*,", content)]
    unique_ids = sorted(set(ids))
    if not unique_ids:
        raise SystemExit(f"No app_user IDs found in seed file: {seed_path}")
    return unique_ids


def main() -> None:
    args = parse_args()

    if args.n <= 0:
        raise SystemExit("--n must be greater than 0")

    total_steps = 7
    python = sys.executable

    distinct_cmd = [python, "data-import/generate_distinct_csvs.py"]

    lookup_cmd = [python, "dml/generate_lookup_seeds.py"]

    ddl_cmd = [python, "run-sql.py", "--scripts-dir", "ddl/tables"]
    if args.sql_connection_string:
        ddl_cmd.insert(2, args.sql_connection_string)

    dml_generate_cmd = [python, "dml/generate_main_seeds.py", "--n", str(args.n)]
    if args.seed is not None:
        dml_generate_cmd.extend(["--seed", str(args.seed)])

    dml_load_cmd = [python, "run-sql.py", "--scripts-dir", "dml/seeds"]
    if args.sql_connection_string:
        dml_load_cmd.insert(2, args.sql_connection_string)

    nosql_load_cmd = [
        python,
        "run-nosql.py",
        "--input-dir",
        "dml/document-seeds",
        "--batch-size",
        str(max(1, args.nosql_batch_size)),
    ]
    if args.nosql_clear:
        nosql_load_cmd.append("--clear")
    if args.nosql_connection_string:
        nosql_load_cmd.insert(2, args.nosql_connection_string)

    run_step(1, total_steps, "Generate distinct CSV files from datasets", distinct_cmd)
    run_step(2, total_steps, "Generate SQL lookup seed files", lookup_cmd)
    run_step(3, total_steps, "Create/ensure SQL schema (DDL)", ddl_cmd)
    run_step(4, total_steps, "Generate SQL main seed files from datasets", dml_generate_cmd)
    run_step(5, total_steps, "Load SQL seed files into PostgreSQL", dml_load_cmd)

    if args.user_ids:
        user_ids_csv = args.user_ids
        print(f"Using user IDs from argument: {user_ids_csv}")
    else:
        app_user_seed_path = ROOT / "dml" / "seeds" / "021_app_user_seed.sql"
        derived_user_ids = parse_user_ids_from_app_user_seed(app_user_seed_path)
        user_ids_csv = ",".join(str(uid) for uid in derived_user_ids)
        print(f"Derived {len(derived_user_ids)} user IDs from {app_user_seed_path.relative_to(ROOT)}")

    doc_generate_cmd = [
        python,
        "dml/generate_document_seeds.py",
        "--user-ids",
        user_ids_csv,
    ]
    if args.sql_connection_string:
        doc_generate_cmd.extend(["--sql-connection-string", args.sql_connection_string])

    run_step(6, total_steps, "Generate NoSQL JSON document seeds", doc_generate_cmd)
    run_step(7, total_steps, "Load NoSQL JSON seeds into MongoDB", nosql_load_cmd)

    print("\nPipeline completed successfully.")


if __name__ == "__main__":
    main()
