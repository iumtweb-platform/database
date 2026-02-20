from __future__ import annotations

import argparse
import ast
import csv
import json
import random
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATA_IMPORT_DIR = ROOT / "data-import"
DATASETS_DIR = DATA_IMPORT_DIR / "datasets"
OUTPUT_DIR = DATA_IMPORT_DIR / "output"
SEEDS_DIR = ROOT / "dml" / "seeds"


ANIME_COLUMNS = [
    "id",
    "type_id",
    "rating_id",
    "season_id",
    "source_id",
    "status_id",
    "title",
    "title_japanese",
    "url",
    "image_url",
    "score",
    "scored_by",
    "start_date",
    "end_date",
    "synopsis",
    "rank",
    "popularity",
    "members",
    "favorites",
    "episodes",
    "year",
    "watching",
    "completed",
    "on_hold",
    "dropped",
    "plan_to_watch",
    "total",
    "score_1_votes",
    "score_1_percentage",
    "score_2_votes",
    "score_2_percentage",
    "score_3_votes",
    "score_3_percentage",
    "score_4_votes",
    "score_4_percentage",
    "score_5_votes",
    "score_5_percentage",
    "score_6_votes",
    "score_6_percentage",
    "score_7_votes",
    "score_7_percentage",
    "score_8_votes",
    "score_8_percentage",
    "score_9_votes",
    "score_9_percentage",
    "score_10_votes",
    "score_10_percentage",
]

CHARACTER_COLUMNS = ["id", "url", "name", "name_japanese", "image_url", "favorites", "about"]
PERSON_COLUMNS = [
    "id",
    "url",
    "website_url",
    "image_url",
    "name",
    "given_name",
    "family_name",
    "birthday",
    "favorites",
    "city",
    "country_id",
]
APP_USER_COLUMNS = ["id", "gender_id", "country_id", "birthday", "joined_date", "username"]
ANIME_GENRE_COLUMNS = ["anime_id", "genre_id"]
ANIME_EXPLICIT_GENRE_COLUMNS = ["anime_id", "explicit_genre_id"]
ANIME_LICENSOR_COLUMNS = ["anime_id", "licensor_id"]
ANIME_DEMOGRAPHIC_COLUMNS = ["anime_id", "demographic_id"]
ANIME_PRODUCER_COLUMNS = ["anime_id", "producer_id"]
ANIME_STREAMING_SERVICE_COLUMNS = ["anime_id", "streaming_service_id"]
ANIME_STUDIO_COLUMNS = ["anime_id", "studio_id"]
ANIME_THEME_COLUMNS = ["anime_id", "theme_id"]
ANIME_RECOMMENDATION_COLUMNS = ["anime_id", "recommended_anime_id"]
CHARACTER_ANIME_WORK_COLUMNS = ["anime_id", "character_id", "character_role_id"]
CHARACTER_NICKNAME_COLUMNS = ["character_id", "nickname"]
PERSON_ANIME_WORK_COLUMNS = ["anime_id", "person_id", "position"]
PERSON_VOICE_WORK_COLUMNS = ["person_id", "anime_id", "character_id", "language_id"]
PERSON_ALTERNATE_NAME_COLUMNS = ["person_id", "alternate_name"]

REQUIRED_ANIME_TEXT = ("title", "title_japanese", "url", "image_url")
REQUIRED_ANIME_IDS = ("source_id", "status_id")
COUNTRY_ALIASES = {
    "USA": "United States",
    "UK": "United Kingdom",
}


class GeneratorError(Exception):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate subset DML seed files for anime/genre/character/character_anime_work"
    )
    parser.add_argument(
        "--n",
        type=int,
        default=None,
        help="Number of anime MAL IDs to sample from output/details/mal_id_distinct.csv",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducible anime ID sampling",
    )
    return parser.parse_args()


def prompt_for_n() -> int:
    while True:
        raw = input("How many anime IDs should be extracted (N)? ").strip()
        try:
            value = int(raw)
        except ValueError:
            print("Please enter an integer value.")
            continue
        if value <= 0:
            print("N must be greater than 0.")
            continue
        return value


def parse_list_value(raw: str) -> list[str]:
    text = raw.strip()
    if not text:
        return []
    if not text.startswith("["):
        return [text]

    parsed: object | None = None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(text)
        except (SyntaxError, ValueError):
            return []

    if not isinstance(parsed, list):
        return []

    values: list[str] = []
    for item in parsed:
        value = str(item).strip()
        if value:
            values.append(value)
    return values


def sql_escape(value: str) -> str:
    return value.replace("'", "''")


def sql_literal(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return f"'{sql_escape(value)}'"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(value)


def read_lookup_map(file_path: Path) -> dict[str, int]:
    mapping: dict[str, int] = {}
    with file_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"id", "value"}
        headers = set(reader.fieldnames or [])
        if not required.issubset(headers):
            raise GeneratorError(f"Lookup CSV must contain columns {sorted(required)}: {file_path}")

        for row in reader:
            raw_id = (row.get("id") or "").strip()
            raw_value = (row.get("value") or "").strip()
            if not raw_id or not raw_value:
                continue
            try:
                value_id = int(raw_id)
            except ValueError as exc:
                raise GeneratorError(f"Invalid lookup id '{raw_id}' in {file_path}") from exc
            mapping[raw_value] = value_id
    return mapping


def parse_int(raw: str | None, default: int | None = None) -> int | None:
    if raw is None:
        return default
    value = raw.strip()
    if value == "":
        return default
    try:
        return int(value)
    except ValueError:
        try:
            return int(float(value))
        except ValueError:
            return default


def parse_float(raw: str | None, default: float | None = None) -> float | None:
    if raw is None:
        return default
    value = raw.strip()
    if value == "":
        return default
    try:
        return float(value)
    except ValueError:
        return default


def parse_date(raw: str | None) -> str | None:
    if raw is None:
        return None
    value = raw.strip()
    if not value:
        return None

    iso_candidate = value[:10]
    try:
        datetime.strptime(iso_candidate, "%Y-%m-%d")
        return iso_candidate
    except ValueError:
        pass

    for fmt in ("%b %d, %Y", "%B %d, %Y", "%b %d,%Y", "%B %d,%Y"):
        try:
            parsed = datetime.strptime(value, fmt)
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            continue

    return None


def normalize_text(raw: str | None) -> str | None:
    if raw is None:
        return None
    value = raw.strip()
    return value or None


def normalize_country_name(raw: str | None) -> str | None:
    value = normalize_text(raw)
    if value is None:
        return None
    return COUNTRY_ALIASES.get(value, value)


def split_location(raw: str | None) -> tuple[str | None, str | None]:
    location = normalize_text(raw)
    if location is None:
        return None, None

    parts = location.rsplit(",", 1)
    if len(parts) == 2:
        city = parts[0].strip() or None
        country = normalize_country_name(parts[1].strip())
        return city, country
    return location, normalize_country_name(location)


def sample_app_users(
    n: int,
    random_seed: int | None,
    gender_map: dict[str, int],
    country_map: dict[str, int],
) -> list[tuple[object, ...]]:
    profiles_path = DATASETS_DIR / "profiles.csv"
    rng = random.Random(None if random_seed is None else random_seed + 1000)

    reservoir: list[tuple[int, dict[str, str]]] = []
    total_rows = 0

    with profiles_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required_cols = {"username", "gender", "birthday", "location", "joined"}
        if not required_cols.issubset(set(reader.fieldnames or [])):
            raise GeneratorError(f"Missing required columns in {profiles_path}")

        for row_idx, row in enumerate(reader, start=1):
            total_rows += 1
            snapshot = {
                "username": (row.get("username") or "").strip(),
                "gender": (row.get("gender") or "").strip(),
                "birthday": (row.get("birthday") or "").strip(),
                "location": (row.get("location") or "").strip(),
                "joined": (row.get("joined") or "").strip(),
            }

            if len(reservoir) < n:
                reservoir.append((row_idx, snapshot))
            else:
                replacement_index = rng.randint(1, row_idx)
                if replacement_index <= n:
                    reservoir[replacement_index - 1] = (row_idx, snapshot)

    if total_rows < n:
        raise GeneratorError(f"Requested N={n} app users, but only {total_rows} profile rows are available")

    app_users: list[tuple[object, ...]] = []
    skipped = 0
    for row_idx, row in sorted(reservoir, key=lambda item: item[0]):
        username = normalize_text(row.get("username"))
        joined_date = parse_date(row.get("joined"))
        country_id = country_map.get(normalize_country_name(row.get("location")) or "")

        if username is None or joined_date is None or country_id is None:
            skipped += 1
            continue

        gender_id = gender_map.get(normalize_text(row.get("gender")) or "")
        birthday = parse_date(row.get("birthday"))

        app_users.append((row_idx, gender_id, country_id, birthday, joined_date, username))

    if skipped:
        print(f"Warning: skipped {skipped} app_user rows due to missing required values")

    return app_users


def choose_anime_ids(n: int, random_seed: int | None) -> set[int]:
    source_path = OUTPUT_DIR / "details" / "mal_id_distinct.csv"
    ids: list[int] = []

    with source_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if "value" not in (reader.fieldnames or []):
            raise GeneratorError(f"Missing 'value' column in {source_path}")
        for row in reader:
            value = parse_int(row.get("value"))
            if value is not None:
                ids.append(value)

    if n > len(ids):
        raise GeneratorError(f"Requested N={n}, but only {len(ids)} anime IDs are available")

    rng = random.Random(random_seed)
    return set(rng.sample(ids, n))


def render_insert_sql(
    table_name: str,
    columns: list[str],
    rows: list[tuple[object, ...]],
    conflict_clause: str,
    generated_by: str,
) -> str:
    header = [
        f"-- Seed data for table: {table_name}",
        f"-- Generated by {generated_by}",
        "",
    ]

    if not rows:
        return "\n".join(header + [f"-- No rows generated for {table_name}.", ""])

    rendered_rows = [
        "    (" + ", ".join(sql_literal(value) for value in row) + ")"
        for row in rows
    ]

    body = [
        f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES",
        ",\n".join(rendered_rows),
        conflict_clause,
        "",
    ]

    return "\n".join(header + body)


def generate() -> None:
    args = parse_args()
    n = args.n if args.n is not None else prompt_for_n()

    if n <= 0:
        raise GeneratorError("N must be greater than 0")

    selected_anime_ids = choose_anime_ids(n, args.seed)
    print(f"Selected {len(selected_anime_ids)} anime IDs")

    type_map = read_lookup_map(OUTPUT_DIR / "details" / "type_distinct.csv")
    rating_map = read_lookup_map(OUTPUT_DIR / "details" / "rating_distinct.csv")
    season_map = read_lookup_map(OUTPUT_DIR / "details" / "season_distinct.csv")
    source_map = read_lookup_map(OUTPUT_DIR / "details" / "source_distinct.csv")
    status_map = read_lookup_map(OUTPUT_DIR / "details" / "status_distinct.csv")
    genre_map = read_lookup_map(OUTPUT_DIR / "details" / "genres_distinct.csv")
    explicit_genre_map = read_lookup_map(OUTPUT_DIR / "details" / "explicit_genres_distinct.csv")
    licensor_map = read_lookup_map(OUTPUT_DIR / "details" / "licensors_distinct.csv")
    demographic_map = read_lookup_map(OUTPUT_DIR / "details" / "demographics_distinct.csv")
    producer_map = read_lookup_map(OUTPUT_DIR / "details" / "producers_distinct.csv")
    streaming_service_map = read_lookup_map(OUTPUT_DIR / "details" / "streaming_distinct.csv")
    studio_map = read_lookup_map(OUTPUT_DIR / "details" / "studios_distinct.csv")
    theme_map = read_lookup_map(OUTPUT_DIR / "details" / "themes_distinct.csv")
    role_map = read_lookup_map(OUTPUT_DIR / "character_anime_works" / "role_distinct.csv")
    country_map = read_lookup_map(OUTPUT_DIR / "profiles" / "location_distinct.csv")
    gender_map = read_lookup_map(OUTPUT_DIR / "profiles" / "gender_distinct.csv")
    language_map = read_lookup_map(OUTPUT_DIR / "person_voice_works" / "language_distinct.csv")

    character_ids_needed: set[int] = set()
    character_anime_rows_map: dict[tuple[int, int], tuple[int, int, int]] = {}

    character_anime_path = DATASETS_DIR / "character_anime_works.csv"
    with character_anime_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required_cols = {"anime_mal_id", "character_mal_id", "role"}
        if not required_cols.issubset(set(reader.fieldnames or [])):
            raise GeneratorError(f"Missing required columns in {character_anime_path}")

        skipped_no_role = 0
        for row in reader:
            anime_id = parse_int(row.get("anime_mal_id"))
            if anime_id is None or anime_id not in selected_anime_ids:
                continue

            character_id = parse_int(row.get("character_mal_id"))
            if character_id is None:
                continue

            role_name = normalize_text(row.get("role"))
            role_id = role_map.get(role_name or "")
            if role_id is None:
                skipped_no_role += 1
                continue

            key = (anime_id, character_id)
            if key in character_anime_rows_map:
                continue

            character_anime_rows_map[key] = (anime_id, character_id, role_id)
            character_ids_needed.add(character_id)

    character_anime_rows = sorted(character_anime_rows_map.values(), key=lambda item: (item[0], item[1]))
    if skipped_no_role:
        print(f"Warning: skipped {skipped_no_role} character_anime_work rows due to unknown role")

    character_rows_map: dict[int, tuple[object, ...]] = {}
    characters_path = DATASETS_DIR / "characters.csv"
    with characters_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required_cols = {"character_mal_id", "url", "name", "name_kanji", "image", "favorites", "about"}
        if not required_cols.issubset(set(reader.fieldnames or [])):
            raise GeneratorError(f"Missing required columns in {characters_path}")

        for row in reader:
            character_id = parse_int(row.get("character_mal_id"))
            if character_id is None or character_id not in character_ids_needed:
                continue

            character_rows_map[character_id] = (
                character_id,
                normalize_text(row.get("url")) or "",
                normalize_text(row.get("name")) or "",
                normalize_text(row.get("name_kanji")),
                normalize_text(row.get("image")) or "",
                parse_int(row.get("favorites"), default=0) or 0,
                normalize_text(row.get("about")),
            )

    character_rows = sorted(character_rows_map.values(), key=lambda item: int(item[0]))

    anime_base_rows: dict[int, dict[str, object]] = {}
    anime_genre_rows_set: set[tuple[int, int]] = set()
    anime_explicit_genre_rows_set: set[tuple[int, int]] = set()
    anime_licensor_rows_set: set[tuple[int, int]] = set()
    anime_demographic_rows_set: set[tuple[int, int]] = set()
    anime_producer_rows_set: set[tuple[int, int]] = set()
    anime_streaming_service_rows_set: set[tuple[int, int]] = set()
    anime_studio_rows_set: set[tuple[int, int]] = set()
    anime_theme_rows_set: set[tuple[int, int]] = set()

    details_path = DATASETS_DIR / "details.csv"
    with details_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required_cols = {
            "mal_id",
            "title",
            "title_japanese",
            "url",
            "image_url",
            "type",
            "status",
            "source",
            "rating",
            "season",
            "score",
            "scored_by",
            "start_date",
            "end_date",
            "synopsis",
            "rank",
            "popularity",
            "members",
            "favorites",
            "episodes",
            "year",
            "genres",
            "explicit_genres",
            "licensors",
            "demographics",
            "producers",
            "streaming",
            "studios",
            "themes",
        }
        if not required_cols.issubset(set(reader.fieldnames or [])):
            raise GeneratorError(f"Missing required columns in {details_path}")

        unknown_genres = 0
        unknown_explicit_genres = 0
        unknown_licensors = 0
        unknown_demographics = 0
        unknown_producers = 0
        unknown_streaming_services = 0
        unknown_studios = 0
        unknown_themes = 0
        for row in reader:
            anime_id = parse_int(row.get("mal_id"))
            if anime_id is None or anime_id not in selected_anime_ids:
                continue

            record: dict[str, object] = {
                "id": anime_id,
                "type_id": type_map.get(normalize_text(row.get("type")) or ""),
                "rating_id": rating_map.get(normalize_text(row.get("rating")) or ""),
                "season_id": season_map.get(normalize_text(row.get("season")) or ""),
                "source_id": source_map.get(normalize_text(row.get("source")) or ""),
                "status_id": status_map.get(normalize_text(row.get("status")) or ""),
                "title": normalize_text(row.get("title")) or "",
                "title_japanese": normalize_text(row.get("title_japanese"))
                or normalize_text(row.get("title"))
                or "",
                "url": normalize_text(row.get("url")) or "",
                "image_url": normalize_text(row.get("image_url")) or "",
                "score": parse_float(row.get("score"), default=0.0) or 0.0,
                "scored_by": parse_float(row.get("scored_by")),
                "start_date": parse_date(row.get("start_date")),
                "end_date": parse_date(row.get("end_date")),
                "synopsis": normalize_text(row.get("synopsis")),
                "rank": parse_float(row.get("rank")),
                "popularity": parse_int(row.get("popularity"), default=0) or 0,
                "members": parse_int(row.get("members"), default=0) or 0,
                "favorites": parse_int(row.get("favorites"), default=0) or 0,
                "episodes": parse_float(row.get("episodes")),
                "year": parse_float(row.get("year")),
            }

            for genre_name in parse_list_value(row.get("genres") or ""):
                genre_id = genre_map.get(genre_name)
                if genre_id is None:
                    unknown_genres += 1
                    continue
                anime_genre_rows_set.add((anime_id, genre_id))

            for name in parse_list_value(row.get("explicit_genres") or ""):
                lookup_id = explicit_genre_map.get(name)
                if lookup_id is None:
                    unknown_explicit_genres += 1
                    continue
                anime_explicit_genre_rows_set.add((anime_id, lookup_id))

            for name in parse_list_value(row.get("licensors") or ""):
                lookup_id = licensor_map.get(name)
                if lookup_id is None:
                    unknown_licensors += 1
                    continue
                anime_licensor_rows_set.add((anime_id, lookup_id))

            for name in parse_list_value(row.get("demographics") or ""):
                lookup_id = demographic_map.get(name)
                if lookup_id is None:
                    unknown_demographics += 1
                    continue
                anime_demographic_rows_set.add((anime_id, lookup_id))

            for name in parse_list_value(row.get("producers") or ""):
                lookup_id = producer_map.get(name)
                if lookup_id is None:
                    unknown_producers += 1
                    continue
                anime_producer_rows_set.add((anime_id, lookup_id))

            for name in parse_list_value(row.get("streaming") or ""):
                lookup_id = streaming_service_map.get(name)
                if lookup_id is None:
                    unknown_streaming_services += 1
                    continue
                anime_streaming_service_rows_set.add((anime_id, lookup_id))

            for name in parse_list_value(row.get("studios") or ""):
                lookup_id = studio_map.get(name)
                if lookup_id is None:
                    unknown_studios += 1
                    continue
                anime_studio_rows_set.add((anime_id, lookup_id))

            for name in parse_list_value(row.get("themes") or ""):
                lookup_id = theme_map.get(name)
                if lookup_id is None:
                    unknown_themes += 1
                    continue
                anime_theme_rows_set.add((anime_id, lookup_id))

            anime_base_rows[anime_id] = record

    if unknown_genres:
        print(f"Warning: skipped {unknown_genres} genre values not found in genre lookup")
    if unknown_explicit_genres:
        print(
            f"Warning: skipped {unknown_explicit_genres} explicit_genre values not found in explicit_genre lookup"
        )
    if unknown_licensors:
        print(f"Warning: skipped {unknown_licensors} licensor values not found in licensor lookup")
    if unknown_demographics:
        print(f"Warning: skipped {unknown_demographics} demographic values not found in demographic lookup")
    if unknown_producers:
        print(f"Warning: skipped {unknown_producers} producer values not found in producer lookup")
    if unknown_streaming_services:
        print(
            "Warning: skipped "
            f"{unknown_streaming_services} streaming_service values not found in streaming_service lookup"
        )
    if unknown_studios:
        print(f"Warning: skipped {unknown_studios} studio values not found in studio lookup")
    if unknown_themes:
        print(f"Warning: skipped {unknown_themes} theme values not found in theme lookup")

    stats_path = DATASETS_DIR / "stats.csv"
    stats_cols = [
        "watching",
        "completed",
        "on_hold",
        "dropped",
        "plan_to_watch",
        "total",
        "score_1_votes",
        "score_1_percentage",
        "score_2_votes",
        "score_2_percentage",
        "score_3_votes",
        "score_3_percentage",
        "score_4_votes",
        "score_4_percentage",
        "score_5_votes",
        "score_5_percentage",
        "score_6_votes",
        "score_6_percentage",
        "score_7_votes",
        "score_7_percentage",
        "score_8_votes",
        "score_8_percentage",
        "score_9_votes",
        "score_9_percentage",
        "score_10_votes",
        "score_10_percentage",
    ]

    stats_found: set[int] = set()
    with stats_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required_cols = {"mal_id", *stats_cols}
        if not required_cols.issubset(set(reader.fieldnames or [])):
            raise GeneratorError(f"Missing required columns in {stats_path}")

        for row in reader:
            anime_id = parse_int(row.get("mal_id"))
            if anime_id is None or anime_id not in anime_base_rows:
                continue

            record = anime_base_rows[anime_id]
            record["watching"] = parse_int(row.get("watching"), default=0) or 0
            record["completed"] = parse_int(row.get("completed"), default=0) or 0
            record["on_hold"] = parse_int(row.get("on_hold"), default=0) or 0
            record["dropped"] = parse_int(row.get("dropped"), default=0) or 0
            record["plan_to_watch"] = parse_int(row.get("plan_to_watch"), default=0) or 0
            record["total"] = parse_int(row.get("total"), default=0) or 0

            for score_idx in range(1, 11):
                votes_col = f"score_{score_idx}_votes"
                perc_col = f"score_{score_idx}_percentage"
                record[votes_col] = parse_float(row.get(votes_col), default=0.0) or 0.0
                record[perc_col] = parse_float(row.get(perc_col), default=0.0) or 0.0

            stats_found.add(anime_id)

    anime_rows: list[tuple[object, ...]] = []
    skipped_anime = 0
    for anime_id in sorted(selected_anime_ids):
        record = anime_base_rows.get(anime_id)
        if record is None:
            print(f"Warning: skipping anime {anime_id} (missing details row)")
            skipped_anime += 1
            continue
        if anime_id not in stats_found:
            print(f"Warning: skipping anime {anime_id} (missing stats row)")
            skipped_anime += 1
            continue

        if any(record.get(name) is None for name in REQUIRED_ANIME_IDS):
            print(f"Warning: skipping anime {anime_id} (missing source/status lookup mapping)")
            skipped_anime += 1
            continue

        if any(not str(record.get(name) or "").strip() for name in REQUIRED_ANIME_TEXT):
            print(f"Warning: skipping anime {anime_id} (missing required text columns)")
            skipped_anime += 1
            continue

        anime_rows.append(tuple(record[col] for col in ANIME_COLUMNS))

    valid_anime_ids = {int(row[0]) for row in anime_rows}
    anime_genre_rows = sorted(
        [row for row in anime_genre_rows_set if row[0] in valid_anime_ids],
        key=lambda item: (item[0], item[1]),
    )
    anime_explicit_genre_rows = sorted(
        [row for row in anime_explicit_genre_rows_set if row[0] in valid_anime_ids],
        key=lambda item: (item[0], item[1]),
    )
    anime_licensor_rows = sorted(
        [row for row in anime_licensor_rows_set if row[0] in valid_anime_ids],
        key=lambda item: (item[0], item[1]),
    )
    anime_demographic_rows = sorted(
        [row for row in anime_demographic_rows_set if row[0] in valid_anime_ids],
        key=lambda item: (item[0], item[1]),
    )
    anime_producer_rows = sorted(
        [row for row in anime_producer_rows_set if row[0] in valid_anime_ids],
        key=lambda item: (item[0], item[1]),
    )
    anime_streaming_service_rows = sorted(
        [row for row in anime_streaming_service_rows_set if row[0] in valid_anime_ids],
        key=lambda item: (item[0], item[1]),
    )
    anime_studio_rows = sorted(
        [row for row in anime_studio_rows_set if row[0] in valid_anime_ids],
        key=lambda item: (item[0], item[1]),
    )
    anime_theme_rows = sorted(
        [row for row in anime_theme_rows_set if row[0] in valid_anime_ids],
        key=lambda item: (item[0], item[1]),
    )

    anime_recommendation_rows_set: set[tuple[int, int]] = set()
    skipped_recommendations = 0
    recommendations_path = DATASETS_DIR / "recommendations.csv"
    with recommendations_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required_cols = {"mal_id", "recommendation_mal_id"}
        if not required_cols.issubset(set(reader.fieldnames or [])):
            raise GeneratorError(f"Missing required columns in {recommendations_path}")

        for row in reader:
            anime_id = parse_int(row.get("mal_id"))
            recommended_anime_id = parse_int(row.get("recommendation_mal_id"))
            if anime_id is None or recommended_anime_id is None:
                continue
            if anime_id not in valid_anime_ids or recommended_anime_id not in valid_anime_ids:
                skipped_recommendations += 1
                continue
            anime_recommendation_rows_set.add((anime_id, recommended_anime_id))

    anime_recommendation_rows = sorted(anime_recommendation_rows_set, key=lambda item: (item[0], item[1]))
    if skipped_recommendations:
        print(
            "Warning: skipped "
            f"{skipped_recommendations} anime_recommendation rows because one or both anime ids are outside the generated subset"
        )
    character_anime_rows = [
        row for row in character_anime_rows if row[0] in valid_anime_ids and row[1] in character_rows_map
    ]

    referenced_character_ids = {row[1] for row in character_anime_rows}
    character_rows = [row for row in character_rows if row[0] in referenced_character_ids]

    character_nickname_rows_set: set[tuple[int, str]] = set()
    character_nickname_path = DATASETS_DIR / "character_nicknames.csv"
    with character_nickname_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required_cols = {"character_mal_id", "nickname"}
        if not required_cols.issubset(set(reader.fieldnames or [])):
            raise GeneratorError(f"Missing required columns in {character_nickname_path}")

        for row in reader:
            character_id = parse_int(row.get("character_mal_id"))
            nickname = normalize_text(row.get("nickname"))
            if character_id is None or nickname is None:
                continue
            if character_id not in referenced_character_ids:
                continue
            character_nickname_rows_set.add((character_id, nickname))

    character_nickname_rows = sorted(character_nickname_rows_set, key=lambda item: (item[0], item[1]))

    person_ids_needed: set[int] = set()
    person_anime_rows_map: dict[tuple[int, int], tuple[int, int, str]] = {}
    person_anime_path = DATASETS_DIR / "person_anime_works.csv"

    with person_anime_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required_cols = {"person_mal_id", "position", "anime_mal_id"}
        if not required_cols.issubset(set(reader.fieldnames or [])):
            raise GeneratorError(f"Missing required columns in {person_anime_path}")

        for row in reader:
            anime_id = parse_int(row.get("anime_mal_id"))
            if anime_id is None or anime_id not in valid_anime_ids:
                continue

            person_id = parse_int(row.get("person_mal_id"))
            if person_id is None:
                continue

            position = normalize_text(row.get("position"))
            if position is None:
                continue

            key = (anime_id, person_id)
            if key in person_anime_rows_map:
                continue

            person_anime_rows_map[key] = (anime_id, person_id, position)
            person_ids_needed.add(person_id)

    person_voice_rows_set: set[tuple[int, int, int, int]] = set()
    skipped_person_voice = 0
    person_voice_path = DATASETS_DIR / "person_voice_works.csv"

    with person_voice_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required_cols = {
            "person_mal_id",
            "anime_mal_id",
            "character_mal_id",
            "language",
        }
        if not required_cols.issubset(set(reader.fieldnames or [])):
            raise GeneratorError(f"Missing required columns in {person_voice_path}")

        for row in reader:
            anime_id = parse_int(row.get("anime_mal_id"))
            if anime_id is None or anime_id not in valid_anime_ids:
                continue

            person_id = parse_int(row.get("person_mal_id"))
            character_id = parse_int(row.get("character_mal_id"))
            if person_id is None or character_id is None:
                continue
            if character_id not in referenced_character_ids:
                continue

            language_name = normalize_text(row.get("language"))
            language_id = language_map.get(language_name or "")
            if language_id is None:
                skipped_person_voice += 1
                continue

            person_voice_rows_set.add((person_id, anime_id, character_id, language_id))
            person_ids_needed.add(person_id)

    if skipped_person_voice:
        print(f"Warning: skipped {skipped_person_voice} person_voice_work rows due to unknown language")

    person_rows_map: dict[int, tuple[object, ...]] = {}
    skipped_person_details = 0
    person_details_path = DATASETS_DIR / "person_details.csv"

    with person_details_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required_cols = {
            "person_mal_id",
            "url",
            "website_url",
            "image_url",
            "name",
            "given_name",
            "family_name",
            "birthday",
            "favorites",
            "relevant_location",
        }
        if not required_cols.issubset(set(reader.fieldnames or [])):
            raise GeneratorError(f"Missing required columns in {person_details_path}")

        for row in reader:
            person_id = parse_int(row.get("person_mal_id"))
            if person_id is None or person_id not in person_ids_needed:
                continue

            city, country_name = split_location(row.get("relevant_location"))
            country_id = country_map.get(country_name or "")
            if city is None or country_id is None:
                skipped_person_details += 1
                continue

            person_rows_map[person_id] = (
                person_id,
                normalize_text(row.get("url")) or "",
                normalize_text(row.get("website_url")),
                normalize_text(row.get("image_url")),
                normalize_text(row.get("name")),
                normalize_text(row.get("given_name")),
                normalize_text(row.get("family_name")),
                parse_date(row.get("birthday")),
                parse_int(row.get("favorites"), default=0) or 0,
                city,
                country_id,
            )

    if skipped_person_details:
        print(f"Warning: skipped {skipped_person_details} person rows due to missing location/country mapping")

    person_rows = sorted(person_rows_map.values(), key=lambda item: int(item[0]))
    valid_person_ids = {int(row[0]) for row in person_rows}

    person_alternate_name_rows_set: set[tuple[int, str]] = set()
    person_alternate_name_path = DATASETS_DIR / "person_alternate_names.csv"
    with person_alternate_name_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required_cols = {"person_mal_id", "alt_name"}
        if not required_cols.issubset(set(reader.fieldnames or [])):
            raise GeneratorError(f"Missing required columns in {person_alternate_name_path}")

        for row in reader:
            person_id = parse_int(row.get("person_mal_id"))
            alternate_name = normalize_text(row.get("alt_name"))
            if person_id is None or alternate_name is None:
                continue
            if person_id not in valid_person_ids:
                continue
            person_alternate_name_rows_set.add((person_id, alternate_name))

    person_alternate_name_rows = sorted(person_alternate_name_rows_set, key=lambda item: (item[0], item[1]))

    app_user_rows = sample_app_users(n=n, random_seed=args.seed, gender_map=gender_map, country_map=country_map)

    person_anime_rows = sorted(
        [row for row in person_anime_rows_map.values() if row[1] in valid_person_ids],
        key=lambda item: (item[0], item[1]),
    )
    person_voice_rows = sorted(
        [
            row
            for row in person_voice_rows_set
            if row[0] in valid_person_ids and row[1] in valid_anime_ids and row[2] in referenced_character_ids
        ],
        key=lambda item: (item[0], item[1], item[2], item[3]),
    )

    if skipped_anime:
        print(f"Skipped {skipped_anime} anime due to missing required dependencies")

    SEEDS_DIR.mkdir(parents=True, exist_ok=True)

    script_name = "dml/generate_main_seeds.py"
    outputs = [
        (
            SEEDS_DIR / "018_character_seed.sql",
            render_insert_sql(
                table_name="character",
                columns=CHARACTER_COLUMNS,
                rows=character_rows,
                conflict_clause="ON CONFLICT (id) DO NOTHING;",
                generated_by=script_name,
            ),
            len(character_rows),
        ),
        (
            SEEDS_DIR / "019_anime_seed.sql",
            render_insert_sql(
                table_name="anime",
                columns=ANIME_COLUMNS,
                rows=anime_rows,
                conflict_clause="ON CONFLICT (id) DO NOTHING;",
                generated_by=script_name,
            ),
            len(anime_rows),
        ),
        (
            SEEDS_DIR / "020_person_seed.sql",
            render_insert_sql(
                table_name="person",
                columns=PERSON_COLUMNS,
                rows=person_rows,
                conflict_clause="ON CONFLICT (id) DO NOTHING;",
                generated_by=script_name,
            ),
            len(person_rows),
        ),
        (
            SEEDS_DIR / "021_app_user_seed.sql",
            render_insert_sql(
                table_name="app_user",
                columns=APP_USER_COLUMNS,
                rows=app_user_rows,
                conflict_clause="ON CONFLICT (id) DO NOTHING;",
                generated_by=script_name,
            ),
            len(app_user_rows),
        ),
        (
            SEEDS_DIR / "022_character_nickname_seed.sql",
            render_insert_sql(
                table_name="character_nickname",
                columns=CHARACTER_NICKNAME_COLUMNS,
                rows=character_nickname_rows,
                conflict_clause="ON CONFLICT (character_id, nickname) DO NOTHING;",
                generated_by=script_name,
            ),
            len(character_nickname_rows),
        ),
        (
            SEEDS_DIR / "023_person_alternate_name_seed.sql",
            render_insert_sql(
                table_name="person_alternate_name",
                columns=PERSON_ALTERNATE_NAME_COLUMNS,
                rows=person_alternate_name_rows,
                conflict_clause="ON CONFLICT (person_id, alternate_name) DO NOTHING;",
                generated_by=script_name,
            ),
            len(person_alternate_name_rows),
        ),
        (
            SEEDS_DIR / "024_anime_genre_seed.sql",
            render_insert_sql(
                table_name="anime_genre",
                columns=ANIME_GENRE_COLUMNS,
                rows=anime_genre_rows,
                conflict_clause="ON CONFLICT (anime_id, genre_id) DO NOTHING;",
                generated_by=script_name,
            ),
            len(anime_genre_rows),
        ),
        (
            SEEDS_DIR / "025_anime_explicit_genre_seed.sql",
            render_insert_sql(
                table_name="anime_explicit_genre",
                columns=ANIME_EXPLICIT_GENRE_COLUMNS,
                rows=anime_explicit_genre_rows,
                conflict_clause="ON CONFLICT (anime_id, explicit_genre_id) DO NOTHING;",
                generated_by=script_name,
            ),
            len(anime_explicit_genre_rows),
        ),
        (
            SEEDS_DIR / "026_anime_licensor_seed.sql",
            render_insert_sql(
                table_name="anime_licensor",
                columns=ANIME_LICENSOR_COLUMNS,
                rows=anime_licensor_rows,
                conflict_clause="ON CONFLICT (anime_id, licensor_id) DO NOTHING;",
                generated_by=script_name,
            ),
            len(anime_licensor_rows),
        ),
        (
            SEEDS_DIR / "027_anime_demographic_seed.sql",
            render_insert_sql(
                table_name="anime_demographic",
                columns=ANIME_DEMOGRAPHIC_COLUMNS,
                rows=anime_demographic_rows,
                conflict_clause="ON CONFLICT (anime_id, demographic_id) DO NOTHING;",
                generated_by=script_name,
            ),
            len(anime_demographic_rows),
        ),
        (
            SEEDS_DIR / "028_anime_producer_seed.sql",
            render_insert_sql(
                table_name="anime_producer",
                columns=ANIME_PRODUCER_COLUMNS,
                rows=anime_producer_rows,
                conflict_clause="ON CONFLICT (anime_id, producer_id) DO NOTHING;",
                generated_by=script_name,
            ),
            len(anime_producer_rows),
        ),
        (
            SEEDS_DIR / "029_anime_streaming_service_seed.sql",
            render_insert_sql(
                table_name="anime_streaming_service",
                columns=ANIME_STREAMING_SERVICE_COLUMNS,
                rows=anime_streaming_service_rows,
                conflict_clause="ON CONFLICT (anime_id, streaming_service_id) DO NOTHING;",
                generated_by=script_name,
            ),
            len(anime_streaming_service_rows),
        ),
        (
            SEEDS_DIR / "030_anime_studio_seed.sql",
            render_insert_sql(
                table_name="anime_studio",
                columns=ANIME_STUDIO_COLUMNS,
                rows=anime_studio_rows,
                conflict_clause="ON CONFLICT (anime_id, studio_id) DO NOTHING;",
                generated_by=script_name,
            ),
            len(anime_studio_rows),
        ),
        (
            SEEDS_DIR / "031_anime_theme_seed.sql",
            render_insert_sql(
                table_name="anime_theme",
                columns=ANIME_THEME_COLUMNS,
                rows=anime_theme_rows,
                conflict_clause="ON CONFLICT (anime_id, theme_id) DO NOTHING;",
                generated_by=script_name,
            ),
            len(anime_theme_rows),
        ),
        (
            SEEDS_DIR / "032_character_anime_work_seed.sql",
            render_insert_sql(
                table_name="character_anime_work",
                columns=CHARACTER_ANIME_WORK_COLUMNS,
                rows=character_anime_rows,
                conflict_clause="ON CONFLICT (anime_id, character_id) DO NOTHING;",
                generated_by=script_name,
            ),
            len(character_anime_rows),
        ),
        (
            SEEDS_DIR / "033_person_anime_work_seed.sql",
            render_insert_sql(
                table_name="person_anime_work",
                columns=PERSON_ANIME_WORK_COLUMNS,
                rows=person_anime_rows,
                conflict_clause="ON CONFLICT (anime_id, person_id) DO NOTHING;",
                generated_by=script_name,
            ),
            len(person_anime_rows),
        ),
        (
            SEEDS_DIR / "034_person_voice_work_seed.sql",
            render_insert_sql(
                table_name="person_voice_work",
                columns=PERSON_VOICE_WORK_COLUMNS,
                rows=person_voice_rows,
                conflict_clause="ON CONFLICT (person_id, anime_id, character_id, language_id) DO NOTHING;",
                generated_by=script_name,
            ),
            len(person_voice_rows),
        ),
        (
            SEEDS_DIR / "035_anime_recommendation_seed.sql",
            render_insert_sql(
                table_name="anime_recommendation",
                columns=ANIME_RECOMMENDATION_COLUMNS,
                rows=anime_recommendation_rows,
                conflict_clause="ON CONFLICT (anime_id, recommended_anime_id) DO NOTHING;",
                generated_by=script_name,
            ),
            len(anime_recommendation_rows),
        ),
    ]

    for out_path, sql_content, row_count in outputs:
        out_path.write_text(sql_content, encoding="utf-8")
        print(f"Wrote {out_path.relative_to(ROOT)} ({row_count} rows)")


if __name__ == "__main__":
    try:
        generate()
    except (GeneratorError, FileNotFoundError) as exc:
        raise SystemExit(f"Error: {exc}")
