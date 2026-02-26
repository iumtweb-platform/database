#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Iterator

from pymongo import MongoClient
from pymongo.errors import BulkWriteError, ConnectionFailure
from tqdm import tqdm


def chunked(items: list[dict[str, Any]], batch_size: int) -> Iterator[list[dict[str, Any]]]:
    for idx in range(0, len(items), batch_size):
        yield items[idx : idx + batch_size]

def load_env_variables() -> None:
    env_path = Path(__file__).resolve().parent / ".env.local"
    if env_path.exists():
        with env_path.open(encoding="utf-8") as env_file:
            for line in env_file:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                key, sep, value = line.partition("=")
                if sep:
                    os.environ[key.strip()] = os.path.expandvars(value.strip())

def resolve_connection_string(connection_string: str | None) -> str:
    if connection_string:
        return connection_string
    database_url = os.getenv("NOSQL_DATABASE_URL")
    if database_url:
        return database_url
    raise ValueError("Missing connection string. Pass it as an argument or set NOSQL_DATABASE_URL.")

def resolve_database_name() -> str:
    database_name = os.getenv("MONGO_DB")
    if database_name:
        return database_name
    raise ValueError("Missing database name. Set MONGO_DB environment variable.")

def load_json_array(path: Path, label: str) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"{label} file not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"{label} file must contain a JSON array: {path}")
    if not all(isinstance(item, dict) for item in payload):
        raise ValueError(f"{label} file must contain an array of JSON objects: {path}")
    return payload


def insert_documents(
    connection_string: str,
    database_name: str,
    users: list[dict[str, Any]],
    ratings: list[dict[str, Any]],
    clear_collections: bool,
    batch_size: int,
) -> None:
    try:
        client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")

        db = client[database_name]
        users_collection = db["users"]
        ratings_collection = db["ratings"]

        if clear_collections:
            users_collection.delete_many({})
            ratings_collection.delete_many({})
            print("Cleared existing documents from users and ratings collections.")

        if users:
            inserted_users = 0
            for batch in tqdm(
                chunked(users, batch_size),
                desc="Inserting users",
                unit="batch",
            ):
                user_result = users_collection.insert_many(batch, ordered=False)
                inserted_users += len(user_result.inserted_ids)
            print(f"Inserted {inserted_users} user documents into {database_name}.users")

            users_collection.create_index("id")
            print("Created index on users.id")
        else:
            print("No user documents to insert.")

        if ratings:
            inserted_ratings = 0
            for batch in tqdm(
                chunked(ratings, batch_size),
                desc="Inserting ratings",
                unit="batch",
            ):
                rating_result = ratings_collection.insert_many(batch, ordered=False)
                inserted_ratings += len(rating_result.inserted_ids)
            print(f"Inserted {inserted_ratings} rating documents into {database_name}.ratings")

            ratings_collection.create_index("id")
            print("Created index on ratings.id")

            ratings_collection.create_index("user_id")
            print("Created index on ratings.user_id")

            ratings_collection.create_index("anime_id")
            print("Created index on ratings.anime_id")

            ratings_collection.create_index("status")
            print("Created index on ratings.status")
        else:
            print("No rating documents to insert.")

        client.close()
    except ConnectionFailure as exc:
        raise RuntimeError(f"Failed to connect to MongoDB: {exc}") from exc
    except BulkWriteError as exc:
        inserted = exc.details.get("nInserted", 0)
        raise RuntimeError(f"Bulk write error after inserting {inserted} documents: {exc.details}") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load JSON document seeds into MongoDB users and ratings collections."
    )
    parser.add_argument(
        "connection_string",
        nargs="?",
        help="MongoDB connection string. Falls back to NOSQL_DATABASE_URL if omitted.",
    )
    parser.add_argument("--database", help="MongoDB database name. Falls back to MONGO_DB if omitted.")
    parser.add_argument(
        "--input-dir",
        default="dml/document-seeds",
        help="Directory containing users.json and ratings.json.",
    )
    parser.add_argument(
        "--users-file",
        help="Optional explicit path to users JSON file. Overrides --input-dir/users.json",
    )
    parser.add_argument(
        "--ratings-file",
        help="Optional explicit path to ratings JSON file. Overrides --input-dir/ratings.json",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Delete existing users and ratings documents before insert.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of documents per insert batch (default: 1000).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_env_variables()

    try:
        connection_string = resolve_connection_string(args.connection_string)
        database_name = resolve_database_name()

        input_dir = Path(args.input_dir)
        users_path = Path(args.users_file) if args.users_file else input_dir / "users.json"
        ratings_path = Path(args.ratings_file) if args.ratings_file else input_dir / "ratings.json"

        users = load_json_array(users_path, "Users")
        ratings = load_json_array(ratings_path, "Ratings")

        insert_documents(
            connection_string=connection_string,
            database_name=database_name,
            users=users,
            ratings=ratings,
            clear_collections=args.clear,
            batch_size=max(1, args.batch_size),
        )

        print("NoSQL load completed successfully.")
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
