# database
Repository to handle ER schema and DDL scripts

## Table creation PostgreSQL

- The SQL scripts for the tables are under [ddl/tables](ddl/tables), ordered by a numbered prefix (`001_...sql`, `002_...sql`, ...).
- Runner: [ddl/create-all.py](ddl/create-all.py).

The script automatically load the environment variables through the .env.local file. First ensure the DB instance is running correctly, then duplicate the .env.example and change the variable DATABASE_URL to the appropriate value.

Dipendenza Python richiesta:

- `pip install psycopg[binary]`

## Generate subset DML seeds

Script: [dml/generate_subset_anime_character_seeds.py](dml/generate_subset_anime_character_seeds.py)

Genera 17 file SQL in [dml/seeds](dml/seeds):

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
- `034_person_voice_work_seed.sql`

Esempio:

```bash
python dml/generate_subset_anime_character_seeds.py --n 100 --seed 42
```

Note:

- Gli anime vengono selezionati casualmente da `data-import/output/details/mal_id_distinct.csv`.
- Gli app user vengono selezionati casualmente con lo stesso valore `N` da `data-import/datasets/profiles.csv`.
- I character vengono filtrati solo da quelli presenti in `character_anime_works.csv` per gli anime selezionati.
- I person vengono filtrati solo da `person_anime_works.csv` e `person_voice_works.csv` per gli anime selezionati.
- Gli insert usano `ON CONFLICT DO NOTHING`.
