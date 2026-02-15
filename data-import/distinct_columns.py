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
        print("Calculating total rows")
        total_rows = sum(1 for _ in handle) - 1  # subtract header
        handle.seek(0)
        reader = csv.DictReader(handle)
        if column not in (reader.fieldnames or []):
            raise ValueError(f"Missing columns in CSV: {column}")

        print("Starting iteration over rows")
        for row in tqdm(reader, total=total_rows):
            raw = (row.get(column) or "").strip()
            if not raw:
                continue

            parsed_list = try_parse_json_list(raw)
            if parsed_list is not None:
                distinct.update(parsed_list)
            else:
                distinct.add(raw)
        
        print("Finished iteration over rows")
    return sorted(distinct)


def main() -> int:
    print("Executing parse_args")
    args = parse_args()
    print("Executed parse_args\n")

    print("Executing parse_columns")
    columns = parse_columns(args.columns)
    print("Executed parse_columns\n")
    if not columns:
        print("No columns provided.", file=sys.stderr)
        return 2

    print("Executing makedirs")
    try:
        os.makedirs(args.output_path, exist_ok=True)
    except OSError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    print("Executed makedirs\n")

    for column in columns:
        print(f"Executing extract_distinct for {column}")
        try:
            values = extract_distinct(args.csv_path, column, args.encoding)
        except (OSError, ValueError) as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1
        print(f"Executed extract_distinct for {column}\n")

        output_file = os.path.join(args.output_path, f"{column}_distinct.txt")
        try:
            with open(output_file, "w", encoding="utf-8") as handle:
                print(f"Executing write to {output_file}")
                for value in values:
                    handle.write(f"{value}\n")
                print(f"Executed write to {output_file}\n")
        except OSError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
