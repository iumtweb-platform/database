#!/usr/bin/env python3
"""Extract distinct values from one or more CSV columns.

If a column value looks like a JSON list (e.g. ["adventure", "drama"]),
this script will split it and include the individual elements.

Example usage:
    python distinct_columns.py \
        --csv-path movies.csv \
        --columns genres,actors \
        --output-path output/distinct_columns
"""

import argparse
import ast
import csv
import json
import os
import sys
from typing import Iterable, List, Set
from tqdm import tqdm


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract distinct values from selected CSV columns."
    )
    parser.add_argument(
        "--csv-path",
        required=True,
        help="Path to the input CSV file.",
    )
    parser.add_argument(
        "--columns",
        required=True,
        help="Comma-separated list of column names to extract distinct values from.",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="CSV file encoding (default: utf-8).",
    )
    parser.add_argument(
        "--output-path",
        required=True,
        help="Path to the output folder.",
    )
    return parser.parse_args()


def parse_columns(value: str) -> List[str]:
    return [col.strip() for col in value.split(",") if col.strip()]


def try_parse_json_list(raw: str) -> Iterable[str] | None:
    """Return list elements if raw is a JSON or Python list, else None."""
    if not raw:
        return None
    if not raw.strip().startswith("["):
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, list):
        return [str(item).strip() for item in parsed if str(item).strip()]
    try:
        parsed = ast.literal_eval(raw)
    except (SyntaxError, ValueError):
        return None
    if isinstance(parsed, list):
        return [str(item).strip() for item in parsed if str(item).strip()]
    return None

def extract_distinct(csv_path: str, column: str, encoding: str) -> List[str]:
    distinct: Set[str] = set()

    with open(csv_path, "r", encoding=encoding, newline="") as handle:
        total_rows = sum(1 for _ in handle) - 1  # subtract header
        handle.seek(0)
        reader = csv.DictReader(handle)
        if column not in (reader.fieldnames or []):
            raise ValueError(f"Missing columns in CSV: {column}")

        for row in tqdm(reader, total=total_rows):
            raw = (row.get(column) or "").strip()
            if not raw:
                continue

            parsed_list = try_parse_json_list(raw)
            if parsed_list is not None:
                distinct.update(parsed_list)
            else:
                distinct.add(raw)
        
    return sorted(distinct)


def main() -> int:
    args = parse_args()

    columns = parse_columns(args.columns)
    if not columns:
        print("No columns provided.", file=sys.stderr)
        return 2

    try:
        os.makedirs(args.output_path, exist_ok=True)
    except OSError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    for column in columns:
        try:
            print(f"\nProcessing column: {column}")
            values = extract_distinct(args.csv_path, column, args.encoding)
        except (OSError, ValueError) as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1

        output_file = os.path.join(args.output_path, f"{column}_distinct.csv")
        try:
            with open(output_file, "w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["id", "value"])
                for idx, value in enumerate(values, start=1):
                    writer.writerow([idx, value])
        except OSError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
