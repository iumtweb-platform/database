# database
Repository to handle ER schema and DDL scripts

## Table creation PostgreSQL

- The SQL scripts for the tables are under [ddl/tables](ddl/tables), ordered by a numbered prefix (`001_...sql`, `002_...sql`, ...).
- Runner: [ddl/create-all.py](ddl/create-all.py).

The script automatically load the environment variables through the .env.local file. First ensure the DB instance is running correctly, then duplicate the .env.example and change the variable DATABASE_URL to the appropriate value.

Dipendenza Python richiesta:

- `pip install psycopg[binary]`

## Generate PostgreSQL DML seeds

Script: [dml/generate_main_seeds.py](dml/generate_main_seeds.py)

Genera 18 file SQL in [dml/seeds](dml/seeds) (018-035):

- `018_character_seed.sql`
- `019_anime_seed.sql`
- `020_person_seed.sql`
- `021_app_user_seed.sql`
- `022_character_nickname_seed.sql`
- `023_person_alternate_name_seed.sql`
- `024_anime_genre_seed.sql`
- `025_anime_explicit_genre_seed.sql`
- `026_anime_licensor_seed.sql`
- `027_anime_demographic_seed.sql`
- `028_anime_producer_seed.sql`
- `029_anime_streaming_service_seed.sql`
- `030_anime_studio_seed.sql`
- `031_anime_theme_seed.sql`
- `032_character_anime_work_seed.sql`
- `033_person_anime_work_seed.sql`
- `035_anime_recommendation_seed.sql`

Esempio:

```bash
python3 dml/generate_main_seeds.py --n 100 --seed 42
```

Note:

- Gli anime vengono selezionati casualmente da `data-import/output/details/mal_id_distinct.csv`.
- Gli app user vengono selezionati casualmente con lo stesso valore `N` da `data-import/datasets/profiles.csv`.
- I character vengono filtrati solo da quelli presenti in `character_anime_works.csv` per gli anime selezionati.
- I person vengono filtrati solo da `person_anime_works.csv` e `person_voice_works.csv` per gli anime selezionati.
- Gli insert usano `ON CONFLICT DO NOTHING`.

## Generate MongoDB user documents

Step 1 script: [dml/generate_document_seeds.py](dml/generate_document_seeds.py)

Step 2 script: [run-nosql.py](run-nosql.py)

Genera documenti utente e rating in MongoDB secondo gli schemi:
- [schema/user_document.json](schema/user_document.json) (collezione `users`)
- [schema/rating_document.json](schema/rating_document.json) (collezione `ratings`)

Dipendenze Python richieste:

- `pip install pymongo psycopg[binary]`

Lo script legge i dati da:
- PostgreSQL `app_user` table (per ottenere i nomi utente dagli ID)
- `data-import/datasets/profiles.csv` (stats: watching, completed, on_hold, dropped, plan_to_watch)
- `data-import/datasets/ratings.csv` (ratings: anime_id, status, score, num_watched_episodes)
- `data-import/datasets/favs.csv` (favorites: anime, characters, people)

Esempio:

```bash
# Step 1: Generate JSON documents
python3 dml/generate_document_seeds.py \
  --user-ids "14,20,33"

# Step 2: Insert generated JSON into MongoDB
python3 run-nosql.py \
  --database anime_db \
  --input-dir dml/document-seeds
```

Note:

- `generate_document_seeds.py` usa `SQL_DATABASE_URL` dal file `.env.local` per leggere `app_user`.
- `run-nosql.py` usa `NOSQL_DATABASE_URL` dal file `.env.local` per inserire in MongoDB.
- Gli ID utente devono corrispondere agli ID nella tabella PostgreSQL `app_user`.
- I documenti utente contengono array di rating IDs, mentre i rating sono documenti separati.
- I rating ID sono generati sequenzialmente (1, 2, 3...).
- I JSON generati sono `dml/document-seeds/users.json`, `dml/document-seeds/ratings.json` e `dml/document-seeds/manifest.json`.

