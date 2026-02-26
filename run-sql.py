from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import psycopg
from tqdm import tqdm


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description="Execute all SQL table scripts in ddl/tables against a Postgres database."
	)
	parser.add_argument(
		"connection_string",
		nargs="?",
		help="PostgreSQL connection string. Falls back to SQL_DATABASE_URL if omitted.",
	)
	parser.add_argument(
		"--scripts-dir",
		required=True,
		help="Directory containing ordered .sql files.",
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


def resolve_connection_string(connection_string: str | None) -> str:
	if connection_string:
		return connection_string
	database_url = os.getenv("SQL_DATABASE_URL")
	if database_url:
		return database_url
	raise ValueError("Missing connection string. Pass it as an argument or set SQL_DATABASE_URL.")


def get_sql_files(scripts_dir: Path) -> list[Path]:
	if not scripts_dir.exists() or not scripts_dir.is_dir():
		raise FileNotFoundError(f"Scripts directory not found: {scripts_dir}")

	sql_files = sorted(path for path in scripts_dir.iterdir() if path.suffix == ".sql")
	if not sql_files:
		raise FileNotFoundError(f"No .sql files found in: {scripts_dir}")
	return sql_files


def execute_sql_files(connection_string: str, sql_files: list[Path], show_progress: bool) -> None:
	with psycopg.connect(connection_string) as connection:
		with connection.cursor() as cursor:
			for sql_file in tqdm(
				sql_files,
				desc="Executing SQL files",
				unit="file",
				disable=not show_progress,
			):
				sql = sql_file.read_text(encoding="utf-8").strip()
				if not sql:
					continue
				try:
					cursor.execute(sql)
				except Exception as exc:
					raise RuntimeError(f"Failed executing {sql_file.name}: {exc}") from exc
		connection.commit()


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

def main() -> None:
	args = parse_args()
	load_env_variables()
	connection_string = resolve_connection_string(args.connection_string)
	sql_files = get_sql_files(Path(args.scripts_dir))
	execute_sql_files(connection_string, sql_files, show_progress=should_enable_tqdm(args.progress))
	print(f"Executed {len(sql_files)} SQL file(s) successfully.")


if __name__ == "__main__":
	main()
