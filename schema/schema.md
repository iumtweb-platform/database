```mermaid
---
config:
    layout: elk
---
erDiagram
    ANIME {
        int id PK
        int type_id FK "nullable"
        int rating_id FK "nullable"
        int season_id FK "nullable"
        int source_id FK
        int status_id FK
        string title
        string title_japanese
        string url
        string image_url
        decimal score "nullable"
        decimal scored_by "nullable"
        date start_date "nullable"
        date end_date "nullable"
        string synopsis "nullable"
        decimal rank "nullable"
        int popularity
        int members
        int favorites
        decimal episodes "nullable"
        decimal year "nullable"
        int watching 
        int completed 
        int on_hold 
        int dropped 
        int plan_to_watch 
        int total 
        decimal score_1_votes 
        decimal score_1_percentage 
        decimal score_2_votes 
        decimal score_2_percentage 
        decimal score_3_votes 
        decimal score_3_percentage 
        decimal score_4_votes 
        decimal score_4_percentage 
        decimal score_5_votes 
        decimal score_5_percentage 
        decimal score_6_votes 
        decimal score_6_percentage 
        decimal score_7_votes 
        decimal score_7_percentage 
        decimal score_8_votes 
        decimal score_8_percentage 
        decimal score_9_votes 
        decimal score_9_percentage 
        decimal score_10_votes 
        decimal score_10_percentage 
    }


    GENRE {
        int id PK
        string genre
    }
    ANIME_GENRE {
        int anime_id PK,FK
        int genre_id PK,FK
    }
    ANIME }|--|{ ANIME_GENRE : 1_to_n
    GENRE }|--|{ ANIME_GENRE : 0_to_n


    EXPLICIT_GENRE {
        int id PK
        string explicit_genre
    }
    ANIME_EXPLICIT_GENRE {
        int anime_id PK,FK
        int explicit_genre_id PK,FK
    }
    ANIME }|--|{ ANIME_EXPLICIT_GENRE : 0_to_n
    EXPLICIT_GENRE }|--|{ ANIME_EXPLICIT_GENRE : 0_to_n


    TYPE {
        int id PK
        string type
    }
    ANIME }|--|{ TYPE : 1_to_1
    

    LICENSOR {
        int id PK
        string licensor
    }
    ANIME_LICENSOR {
        int anime_id PK,FK
        int licensor_id PK,FK
    }
    LICENSOR }|--|{ ANIME_LICENSOR : 0_to_n
    ANIME }|--|{ ANIME_LICENSOR : 0_to_n


    DEMOGRAPHIC {
        int id PK
        string demographic
    }
    ANIME_DEMOGRAPHIC {
        int anime_id PK,FK
        int demographic_id PK,FK
    }
    DEMOGRAPHIC }|--|{ ANIME_DEMOGRAPHIC : 0_to_n
    ANIME }|--|{ ANIME_DEMOGRAPHIC : 0_to_n


    PRODUCER {
        int id PK
        string producer
    }
    ANIME_PRODUCER {
        int anime_id PK,FK
        int producer_id PK,FK
    }
    PRODUCER }|--|{ ANIME_PRODUCER : 0_to_n
    ANIME }|--|{ ANIME_PRODUCER : 0_to_n


    RATING {
        int id PK
        string rating
    }
    ANIME }|--|{ RATING : 1_to_1


    SEASON {
        int id PK
        string season
    }
    ANIME }|--|{ SEASON : 1_to_1


    SOURCE {
        int id PK
        string source
    }
    ANIME }|--|{ SOURCE : 1_to_1


    STREAMING_SERVICE {
        int id PK
        string streaming_service
    }
    ANIME_STREAMING_SERVICE {
        int anime_id PK,FK
        int streaming_service_id PK,FK
    }
    STREAMING_SERVICE }|--|{ ANIME_STREAMING_SERVICE : 0_to_n
    ANIME }|--|{ ANIME_STREAMING_SERVICE : 0_to_n


    STATUS {
        int id PK
        string status
    }
    ANIME }|--|{ STATUS : 1_to_1


    STUDIO {
        int id PK
        string studio
    }
    ANIME_STUDIO {
        int anime_id PK,FK
        int studio_id PK,FK
    }
    STUDIO }|--|{ ANIME_STUDIO : 0_to_n
    ANIME }|--|{ ANIME_STUDIO : 0_to_n


    THEME {
        int id PK
        string theme
    }
    ANIME_THEME {
        int anime_id PK,FK
        int theme_id PK,FK
    }
    THEME }|--|{ ANIME_THEME : 0_to_n
    ANIME }|--|{ ANIME_THEME : 0_to_n



    CHARACTER {
        int id PK
        string url
        string name
        string name_kanji "nullable"
        string image
        int favorites
        string about "nullable"
    }
    CHARACTER_ANIME_WORK {
        int anime_id PK,FK
        int character_id PK,FK
        int character_role_id FK
    }
    CHARACTER_ROLE {
        int id PK
        string role
    }
    CHARACTER_NICKNAME {
        int character_id PK,FK
        string nickname PK
    }
    ANIME }|--|{ CHARACTER_ANIME_WORK : 1_to_n
    CHARACTER }|--|{ CHARACTER_NICKNAME : 0_to_n
    CHARACTER }|--|{ CHARACTER_ANIME_WORK : 0_to_n
    CHARACTER_ANIME_WORK }|--|{ CHARACTER_ROLE : 1_to_1



    COUNTRY {
        int id PK
        string country
    }



    PERSON {
        int id PK
        string url
        string website_url "nullable"
        string image_url "nullable"
        string name "nullable"
        string given_name "nullable"
        string family_name "nullable"
        date birthday "nullable"
        int favorites
        string city
        int country_id FK
    }
    PERSON_ANIME_WORK {
        int anime_id PK,FK
        int person_id PK,FK
        string position
    }
    PERSON_VOICE_WORK {
        int person_id PK,FK
        int anime_id PK,FK
        int character_id PK,FK
        int language_id PK,FK
    }
    PERSON_ALTERNATE_NAME {
        int person_id PK,FK
        string alternate_name PK
    }
    LANGUAGE {
        int id PK
        string language
    }
    PERSON }|--|{ PERSON_VOICE_WORK : 0_to_n
    PERSON_VOICE_WORK }|--|{ LANGUAGE : 1_to_1
    PERSON }|--|{ PERSON_ANIME_WORK : 0_to_n
    PERSON }|--|{ PERSON_ALTERNATE_NAME : 0_to_n
    PERSON }|--|{ COUNTRY : 1_to_1



    USER {  
        int id PK
        int gender_id FK "nullable"
        int country_id FK
        date birthday "nullable"
        date joined_date
        string username
    }
    GENDER {
        int id PK
        string gender
    }
    USER }|--|{ COUNTRY : 1_to_1
    USER }|--|{ GENDER : 0_to_1



    ANIME_RECOMMENDATION {
        int anime_id PK,FK
        int recommended_anime_id PK,FK
    }
    ANIME }|--|{ ANIME_RECOMMENDATION : 1_to_n
```