CREATE TABLE character (
    id integer PRIMARY KEY,
    url text NOT NULL,
    name varchar(255) NOT NULL,
    name_japanese varchar(255),
    image_url text NOT NULL,
    favorites integer NOT NULL,
    about varchar(16383)
);