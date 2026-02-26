#!/usr/bin/env python3
"""
Generate NoSQL JSON seed documents from CSV datasets.

Step 1 only: create JSON files on disk.
Use run-nosql.py as Step 2 to load generated JSON into MongoDB.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import psycopg
from tqdm import tqdm


DATASETS_DIR = Path(__file__).resolve().parent.parent / "data-import" / "datasets"
OUTPUT_DIR = Path(__file__).resolve().parent / "document-seeds"
PROFILES_CSV = DATASETS_DIR / "profiles.csv"
RATINGS_CSV = DATASETS_DIR / "ratings.csv"
FAVS_CSV = DATASETS_DIR / "favs.csv"


def count_data_rows(file_path: Path) -> int:
    with file_path.open("r", encoding="utf-8") as file:
        return max(sum(1 for _ in file) - 1, 0)


def load_env_variables() -> None:
    env_path = Path(__file__).resolve().parent.parent / ".env.local"
    if env_path.exists():
        with env_path.open(encoding="utf-8") as env_file:
            for line in env_file:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                key, sep, value = line.partition("=")
                if sep:
                    os.environ[key.strip()] = os.path.expandvars(value.strip())


def resolve_sql_connection_string(connection_string: str | None) -> str:
    if connection_string:
        return connection_string
    database_url = os.getenv("SQL_DATABASE_URL")
    if database_url:
        return database_url
    raise ValueError(
        "Missing PostgreSQL connection string. Pass it with --sql-connection-string "
        "or set SQL_DATABASE_URL environment variable."
    )


def fetch_usernames_from_db(sql_connection_string: str, user_ids: list[int]) -> dict[int, str]:
    try:
        with psycopg.connect(sql_connection_string) as conn:
            with conn.cursor() as cur:
                placeholders = ",".join(["%s"] * len(user_ids))
                query = f"SELECT id, username FROM app_user WHERE id IN ({placeholders})"
                cur.execute(query, user_ids)
                rows = cur.fetchall()
                return {int(row[0]): str(row[1]) for row in rows}
    except Exception as exc:
        print(f"Error: Failed to fetch usernames from PostgreSQL: {exc}", file=sys.stderr)
        sys.exit(1)


def parse_int(value: str | None) -> int:
    if not value or value.strip() == "":
        return 0
    cleaned = value.replace(",", "").strip()
    try:
        if "." in cleaned:
            return int(float(cleaned))
        return int(cleaned)
    except (ValueError, TypeError):
        return 0


def normalize_status(status: str) -> str:
    if not status:
        return ""
    return status.strip().replace(" ", "_").lower()


def should_enable_tqdm(mode: str) -> bool:
    if mode == "off":
        return False
    if mode == "linear":
        return sys.stdout.isatty()
    return True


def load_profiles(usernames: set[str], show_progress: bool) -> dict[str, dict[str, Any]]:
    profiles: dict[str, dict[str, Any]] = {}
    total_rows = count_data_rows(PROFILES_CSV)
    with PROFILES_CSV.open("r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in tqdm(
            reader,
            total=total_rows,
            desc="Loading profiles",
            unit="row",
            disable=not show_progress,
        ):
            username = row.get("username", "")
            if username in usernames:
                profiles[username] = {
                    "stats": {
                        "watching": parse_int(row.get("watching")),
                        "completed": parse_int(row.get("completed")),
                        "on_hold": parse_int(row.get("on_hold")),
                        "dropped": parse_int(row.get("dropped")),
                        "plan_to_watch": parse_int(row.get("plan_to_watch")),
                    }
                }
    return profiles


def load_ratings(usernames: set[str], show_progress: bool) -> dict[str, list[dict[str, int | str]]]:
    ratings: dict[str, list[dict[str, int | str]]] = defaultdict(list)
    total_rows = count_data_rows(RATINGS_CSV)
    with RATINGS_CSV.open("r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in tqdm(
            reader,
            total=total_rows,
            desc="Loading ratings",
            unit="row",
            disable=not show_progress,
        ):
            username = row.get("username", "")
            if username in usernames:
                ratings[username].append(
                    {
                        "anime_id": parse_int(row.get("anime_id")),
                        "status": normalize_status(row.get("status", "")),
                        "score": parse_int(row.get("score")),
                        "num_watched_episodes": parse_int(row.get("num_watched_episodes")),
                    }
                )
    return dict(ratings)


def load_favorites(usernames: set[str], show_progress: bool) -> dict[str, dict[str, list[int]]]:
    favorites: dict[str, dict[str, list[int]]] = defaultdict(
        lambda: {"anime": [], "characters": [], "people": []}
    )
    total_rows = count_data_rows(FAVS_CSV)
    with FAVS_CSV.open("r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in tqdm(
            reader,
            total=total_rows,
            desc="Loading favorites",
            unit="row",
            disable=not show_progress,
        ):
            username = row.get("username", "")
            if username in usernames:
                fav_type = str(row.get("fav_type", "")).strip()
                fav_id = parse_int(row.get("id"))
                if fav_id <= 0:
                    continue
                if fav_type == "anime":
                    favorites[username]["anime"].append(fav_id)
                elif fav_type == "character":
                    favorites[username]["characters"].append(fav_id)
                elif fav_type in {"people", "person"}:
                    favorites[username]["people"].append(fav_id)
    return dict(favorites)


def build_rating_documents(
    ratings_data: dict[str, list[dict[str, int | str]]],
    user_id_to_username: dict[int, str],
    show_progress: bool,
) -> tuple[dict[int, list[int]], list[dict[str, int | str]]]:
    rating_documents: list[dict[str, int | str]] = []
    user_rating_ids: dict[int, list[int]] = defaultdict(list)
    current_rating_id = 1

    for user_id in tqdm(
        sorted(user_id_to_username.keys()),
        desc="Building rating documents",
        unit="user",
        disable=not show_progress,
    ):
        username = user_id_to_username[user_id]
        for rating_data in ratings_data.get(username, []):
            rating_doc: dict[str, int | str] = {
                "id": current_rating_id,
                "user_id": user_id,
                "anime_id": int(rating_data["anime_id"]),
                "status": str(rating_data["status"]),
                "score": int(rating_data["score"]),
                "num_watched_episodes": int(rating_data["num_watched_episodes"]),
            }
            rating_documents.append(rating_doc)
            user_rating_ids[user_id].append(current_rating_id)
            current_rating_id += 1

    return dict(user_rating_ids), rating_documents


def build_user_documents(
    user_id_to_username: dict[int, str],
    profiles_data: dict[str, dict[str, Any]],
    user_rating_ids: dict[int, list[int]],
    favorites_data: dict[str, dict[str, list[int]]],
    show_progress: bool,
) -> list[dict[str, Any]]:
    user_documents: list[dict[str, Any]] = []

    for user_id in tqdm(
        sorted(user_id_to_username.keys()),
        desc="Building user documents",
        unit="user",
        disable=not show_progress,
    ):
        username = user_id_to_username[user_id]
        profile = profiles_data.get(
            username,
            {
                "stats": {
                    "watching": 0,
                    "completed": 0,
                    "on_hold": 0,
                    "dropped": 0,
                    "plan_to_watch": 0,
                }
            },
        )
        favorites = favorites_data.get(
            username,
            {"anime": [], "characters": [], "people": []},
        )
        user_documents.append(
            {
                "id": user_id,
                "stats": profile["stats"],
                "ratings": user_rating_ids.get(user_id, []),
                "favorites": favorites,
            }
        )

    return user_documents


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)


def parse_user_ids(raw_ids: str) -> list[int]:
    try:
        ids = [int(item.strip()) for item in raw_ids.split(",") if item.strip()]
    except ValueError:
        raise ValueError("Invalid user IDs provided. Must be comma-separated integers.") from None
    if not ids:
        raise ValueError("No valid user IDs provided.")
    return ids


def parse_user_ids_file(path: str) -> list[int]:
    user_ids_path = Path(path)
    if not user_ids_path.exists() or not user_ids_path.is_file():
        raise ValueError(f"User IDs file not found: {user_ids_path}")

    raw_content = user_ids_path.read_text(encoding="utf-8").strip()
    if not raw_content:
        raise ValueError(f"User IDs file is empty: {user_ids_path}")

    normalized = raw_content.replace("\n", ",").replace("\r", ",")
    return parse_user_ids(normalized)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate NoSQL JSON seed documents.")
    user_id_group = parser.add_mutually_exclusive_group(required=True)
    user_id_group.add_argument(
        "--user-ids",
        help="Comma-separated list of app_user IDs from PostgreSQL (e.g., 14,20,33)",
    )
    user_id_group.add_argument(
        "--user-ids-file",
        help="Path to a file containing app_user IDs (comma or newline separated).",
    )
    parser.add_argument(
        "--sql-connection-string",
        help="PostgreSQL connection string. Falls back to SQL_DATABASE_URL if omitted.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(OUTPUT_DIR),
        help="Output directory for generated JSON files (default: dml/document-seeds)",
    )
    parser.add_argument(
        "--progress",
        choices=("linear", "detailed", "off"),
        default="detailed",
        help="Progress display mode (default: detailed).",
    )

    args = parser.parse_args()
    load_env_variables()
    show_progress = should_enable_tqdm(args.progress)

    try:
        if args.user_ids_file:
            user_ids = parse_user_ids_file(args.user_ids_file)
        else:
            user_ids = parse_user_ids(args.user_ids)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    sql_connection_string = resolve_sql_connection_string(args.sql_connection_string)
    user_id_to_username = fetch_usernames_from_db(sql_connection_string, user_ids)

    missing_ids = sorted(set(user_ids) - set(user_id_to_username.keys()))
    if missing_ids:
        print(f"Warning: Some user IDs were not found in app_user: {missing_ids}", file=sys.stderr)

    if not user_id_to_username:
        print("Error: No valid users found in app_user for provided IDs.", file=sys.stderr)
        sys.exit(1)

    usernames = set(user_id_to_username.values())
    profiles_data = load_profiles(usernames, show_progress=show_progress)
    ratings_data = load_ratings(usernames, show_progress=show_progress)
    favorites_data = load_favorites(usernames, show_progress=show_progress)

    user_rating_ids, rating_documents = build_rating_documents(
        ratings_data,
        user_id_to_username,
        show_progress=show_progress,
    )
    user_documents = build_user_documents(
        user_id_to_username,
        profiles_data,
        user_rating_ids,
        favorites_data,
        show_progress=show_progress,
    )

    output_dir = Path(args.output_dir)
    users_path = output_dir / "users.json"
    ratings_path = output_dir / "ratings.json"
    manifest_path = output_dir / "manifest.json"

    write_json(users_path, user_documents)
    write_json(ratings_path, rating_documents)
    write_json(
        manifest_path,
        {
            "users_file": str(users_path),
            "ratings_file": str(ratings_path),
            "users_count": len(user_documents),
            "ratings_count": len(rating_documents),
            "user_ids": sorted(user_id_to_username.keys()),
        },
    )

    print(f"Generated users JSON: {users_path}")
    print(f"Generated ratings JSON: {ratings_path}")
    print(f"Generated manifest JSON: {manifest_path}")
    print(f"Total: {len(user_documents)} user documents, {len(rating_documents)} rating documents")


if __name__ == "__main__":
    main()
