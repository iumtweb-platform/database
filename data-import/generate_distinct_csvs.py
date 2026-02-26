#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from tqdm import tqdm


ROOT = Path(__file__).resolve().parents[1]


JOBS: list[tuple[str, str, str]] = [
    (
        "data-import/datasets/details.csv",
        "mal_id,type,rating,season,source,status,genres,explicit_genres,licensors,demographics,producers,streaming,studios,themes",
        "data-import/output/details",
    ),
    (
        "data-import/datasets/character_anime_works.csv",
        "role",
        "data-import/output/character_anime_works",
    ),
    (
        "data-import/datasets/profiles.csv",
        "gender,location",
        "data-import/output/profiles",
    ),
    (
        "data-import/datasets/person_voice_works.csv",
        "language",
        "data-import/output/person_voice_works",
    ),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate all distinct CSV files required by lookup/main seed generators."
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="CSV file encoding passed to distinct_columns.py (default: utf-8).",
    )
    parser.add_argument(
        "--progress",
        choices=("linear", "detailed", "off"),
        default="detailed",
        help="Progress display mode (default: detailed).",
    )
    return parser.parse_args()


def should_enable_tqdm(mode: str) -> bool:
    if mode == "off":
        return False
    if mode == "linear":
        return sys.stdout.isatty()
    return True


def main() -> None:
    args = parse_args()
    python = sys.executable
    script = ROOT / "data-import" / "distinct_columns.py"

    if not script.exists():
        raise SystemExit(f"Missing script: {script}")

    for csv_path, columns, output_path in tqdm(
        JOBS,
        desc="Generating distinct CSV groups",
        unit="group",
        disable=not should_enable_tqdm(args.progress),
    ):
        command = [
            python,
            str(script),
            "--csv-path",
            str(ROOT / csv_path),
            "--columns",
            columns,
            "--encoding",
            args.encoding,
            "--output-path",
            str(ROOT / output_path),
            "--progress",
            args.progress,
        ]
        print("$ " + " ".join(command))
        try:
            subprocess.run(command, cwd=ROOT, check=True)
        except subprocess.CalledProcessError as exc:
            raise SystemExit(
                f"Failed generating distinct CSVs for {csv_path} (exit code {exc.returncode})"
            ) from exc

    print("All required distinct CSV files were generated successfully.")


if __name__ == "__main__":
    main()
