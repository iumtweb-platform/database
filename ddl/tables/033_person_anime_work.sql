CREATE TABLE person_anime_work (
    anime_id integer NOT NULL,
    person_id integer NOT NULL,
    position varchar(256) NOT NULL,
    PRIMARY KEY (anime_id, person_id),
    CONSTRAINT fk_person_anime_work_anime FOREIGN KEY (anime_id) REFERENCES anime (id) ON DELETE CASCADE,
    CONSTRAINT fk_person_anime_work_person FOREIGN KEY (person_id) REFERENCES person (id) ON DELETE CASCADE
);