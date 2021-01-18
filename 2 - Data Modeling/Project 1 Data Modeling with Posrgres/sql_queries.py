# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

user_table_create = (""" CREATE TABLE IF NOT EXISTS users (
                            user_id int PRIMARY KEY,
                            first_name varchar,
                            last_name varchar,
                            gender varchar,
                            level varchar);
""")


artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists (
                                artist_id varchar UNIQUE,
                                name varchar,
                                location varchar,
                                latitude double precision,
                                longitude double precision);
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (
                            start_time timestamp PRIMARY KEY,
                            hour int,
                            day int,
                            week int,
                            month int,
                            year int,
                            weekday int);
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs (
                            song_id varchar UNIQUE,
                            title varchar,
                            artist_id varchar,
                            year int,
                            duration double precision);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                            songplay_id serial PRIMARY KEY,
                            start_time bigint,
                            user_id int NOT NULL,
                            level varchar,
                            song_id varchar,
                            artist_id varchar,
                            session_id int,
                            location varchar,
                            user_agent varchar,
                            FOREIGN KEY (user_id) REFERENCES users(user_id));
""")

# INSERT RECORDS

songplay_table_insert = (""" INSERT INTO songplays (
                            start_time,
                            user_id,
                            level,
                            song_id,
                            artist_id,
                            session_id,
                            location,
                            user_agent)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (songplay_id) DO NOTHING;
                            
""")

user_table_insert = (""" INSERT INTO users (
                            user_id,
                            first_name,
                            last_name,
                            gender,
                            level)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;
""")

song_table_insert = (""" INSERT INTO songs (
                            song_id,
                            title,
                            artist_id,
                            year,
                            duration)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = (""" INSERT INTO artists (
                                artist_id,
                                name,
                                location,
                                latitude,
                                longitude)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (artist_id) DO NOTHING;
""")


time_table_insert = (""" INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (start_time) DO NOTHING;""")

# FIND SONGS

song_select = ("""SELECT
                      song.song_id,
                      artist.artist_id
                    FROM
                      (SELECT
                        artist_id
                       FROM
                        artists
                       WHERE
                        name = %s) AS artist 
                      LEFT JOIN
                      (SELECT
                        song_id,
                        artist_id
                      FROM
                        songs
                      WHERE
                        title = %s AND duration = %s) AS song 
                      ON song.artist_id = artist.artist_id
""")

# QUERY LISTS

create_table_queries = [user_table_create, time_table_create, artist_table_create, song_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]