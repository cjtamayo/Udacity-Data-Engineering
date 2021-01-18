import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

log_data, log_json, song_data = config['S3']['LOG_DATA'], config['S3']['LOG_JSONPATH'], config['S3']['SONG_DATA']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = (""" CREATE TABLE IF NOT EXISTS staging_events (
                                    artist TEXT,
                                    auth TEXT,
                                    firstName TEXT,
                                    gender CHAR,
                                    itemInSession INTEGER,
                                    lastName TEXT,
                                    length DECIMAL(20,5),
                                    level VARCHAR(10),
                                    location TEXT,
                                    method VARCHAR(10),
                                    page varchar(20),
                                    registration TEXT,
                                    sessionId INTEGER,
                                    song TEXT,
                                    status INTEGER,
                                    ts BIGINT,
                                    userAgent TEXT,
                                    userId INTEGER
                                    )

""")


staging_songs_table_create= (""" CREATE TABLE IF NOT EXISTS staging_songs (
                                    num_songs INTEGER,
                                    artist_id VARCHAR(20),
                                    artist_latitude DECIMAL(10,7),
                                    artist_longitude DECIMAL(10,7),
                                    artist_location TEXT,
                                    artist_name TEXT,
                                    song_id VARCHAR(25),
                                    title TEXT,
                                    duration DECIMAL(20,5),
                                    year INTEGER
                                    )
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id INTEGER PRIMARY KEY IDENTITY(0,1) NOT NULL, 
                                start_time TIMESTAMP NOT NULL, 
                                user_id INTEGER NOT NULL, 
                                level VARCHAR(10), 
                                song_id VARCHAR(20) NOT NULL, 
                                artist_id VARCHAR(20) NOT NULL, 
                                session_id INTEGER NOT NULL, 
                                location TEXT, 
                                user_agent TEXT
                            )
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                            user_id INTEGER PRIMARY KEY sortkey,
                            first_name VARCHAR(25),
                            last_name VARCHAR(25),
                            gender CHAR,
                            level TEXT)
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs (
                            song_id VARCHAR(25) PRIMARY KEY NOT NULL sortkey,
                            title TEXT,
                            artist_id VARCHAR(20) NOT NULL,
                            year INTEGER,
                            duration DECIMAL(10,5))
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                                artist_id VARCHAR(20) PRIMARY KEY sortkey,
                                name TEXT,
                                location TEXT,
                                latitude DECIMAL(10,7),
                                longitude DECIMAL(10,7))
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (
                            start_time TIMESTAMP PRIMARY KEY NOT NULL sortkey,
                            hour INTEGER,
                            day INTEGER,
                            week INTEGER,
                            month INTEGER,
                            year INTEGER,
                            weekday INTEGER)
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {}
                            credentials 'aws_iam_role={}'
                            region 'us-west-2' json {} truncatecolumns;
                      """).format(log_data, *config['IAM_ROLE'].values(), log_json)

staging_songs_copy = ("""copy staging_songs from {}
                            credentials 'aws_iam_role={}'
                            region 'us-west-2' json 'auto' truncatecolumns;
                      """).format(song_data, *config['IAM_ROLE'].values())

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)(
                                SELECT DISTINCT
                                  a.start_time,
                                  a.userId,
                                  a.level,
                                  b.song_id,
                                  b.artist_id,
                                  a.sessionId,
                                  a.location,
                                  a.userAgent
                                FROM
                                  staging_songs b
                                  INNER JOIN (
                                  SELECT DISTINCT
                                    (timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second') AS start_time ,
                                    userId,
                                    level,
                                    sessionId,
                                    location,
                                    userAgent,
                                    artist,
                                    song,
                                    length
                                  FROM
                                    staging_events
                                  WHERE
                                      page = 'NextSong') a ON b.artist_name = a.artist 
                                                          AND b.title = a.song 
                                                          AND a.length = b.duration)
                                
""")

user_table_insert = (""" INSERT INTO users (
                                SELECT
                                  userId,
                                  firstName,
                                  lastName,
                                  gender,
                                  level
                                FROM
                                  staging_events
                                WHERE
                                  page = 'NextSong'
                            )
""")

song_table_insert = (""" INSERT INTO songs (
                            SELECT DISTINCT
                              song_id,
                              title,
                              artist_id,
                              year,
                              duration
                            FROM
                              staging_songs
                    )

""")

artist_table_insert = (""" INSERT INTO artists (
                            SELECT DISTINCT
                              artist_id,
                              artist_name,
                              artist_location,
                              artist_latitude,
                              artist_longitude
                            FROM
                              staging_songs
                        )
""")

time_table_insert = (""" INSERT INTO time (
                            SELECT DISTINCT
                              start_time,
                              EXTRACT(hour FROM start_time),
                              EXTRACT(day FROM start_time),
                              EXTRACT(week FROM start_time),
                              EXTRACT(month FROM start_time),
                              EXTRACT(year FROM start_time),
                              EXTRACT(dayofweek FROM start_time)
                            FROM
                              (
                            SELECT DISTINCT
                              (timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second') AS start_time
                            FROM
                              staging_events) 
                        )
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
