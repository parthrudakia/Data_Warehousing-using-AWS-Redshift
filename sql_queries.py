import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artist128time_table_drop = "DROP TABLE IF EXISTS time_of_use"

# CREATE TABLES

staging_events_table_create= ("""
                                CREATE TABLE IF NOT EXISTS staging_events
                                (
                                    artist varchar(128),
                                    auth varchar(128),
                                    firstName varchar(128),
                                    gender char,
                                    itemInSession int,
                                    lastName varchar(128),
                                    length float,
                                    level varchar(128),
                                    location varchar(128),
                                    method varchar(128),
                                    page varchar(128),
                                    registration float,
                                    sessionId int,
                                    song varchar(128),
                                    status int,
                                    ts bigint,
                                    userAgent varchar(128),
                                    userId varchar(128)
                                )
                            """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
                                (
                                    num_songs int,
                                    artist_id varchar(128),
                                    artist_latitude float,
                                    artist_longitude float,
                                    artist_location varchar(128),
                                    artist_name varchar(128),
                                    song_id varchar(128),
                                    title varchar(128),
                                    duration float,
                                    year int
                                );
""")

songplay_table_create = ("""
                        CREATE TABLE IF NOT EXISTS songplays 
                            (
                            songplay_id bigint IDENTITY(0,1) PRIMARY KEY distkey,
                            start_time timestamp NOT NULL, 
                            user_id varchar(128) NOT NULL, 
                            level varchar(128), 
                            song_id varchar(128),
                            artist_id varchar(128) NOT NULL, 
                            session_id int, 
                            location varchar(128),
                            user_agent varchar(128)
                            );
""")


user_table_create = ("""CREATE TABLE IF NOT EXISTS users 
                        (
                            user_id varchar(128) PRIMARY KEY,
                            first_name varchar(128), 
                            last_name varchar(128), 
                            gender char, 
                            level varchar(128)
                        );
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs 
                        (
                            song_id varchar(128) PRIMARY KEY, 
                            title varchar(128),
                            artist_id varchar(128), 
                            year int, 
                            duration float
                        );
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists 
                        (
                            artist_id varchar(128) PRIMARY KEY,
                            name varchar(128), 
                            location varchar(128), 
                            latitude float, 
                            longitude float
                       );
""")


time_table_create = ("""CREATE TABLE IF NOT EXISTS time_of_use 
                        (
                            start_time timestamp PRIMARY KEY,
                            hour int, 
                            day int, 
                            week int, 
                            month int, 
                            year int,
                            weekday int
                        );
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {}
                            iam_role {}
                            format as json {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""COPY staging_songs FROM {}
                        iam_role {}
                        format as json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, 
                            user_id, level, song_id, artist_id, session_id,
                            location, user_agent)
                            SELECT DISTINCT(DATEADD(s, e.ts/1000, '19700101')) AS start_time,
                            e.userId AS user_id,
                            e.level,
                            s.song_id,
                            s.artist_id,
                            e.sessionId AS session_id,
                            e.location,
                            e.userAgent AS user_agent
                            FROM staging_events e
                            JOIN staging_songs s ON (e.song = s.title
                            AND e.length = s.duration
                            AND e.artist = s.artist_name)
                            WHERE e.page='NextSong'
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name,
                        gender, level)
                        SELECT DISTINCT s.userId AS user_id,
                        s.firstName AS first_name,
                        s.lastName AS last_name,
                        s.gender,
                        s.level
                        FROM staging_events s
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, 
                        year, duration)
                        SELECT DISTINCT song_id,
                        title,
                        artist_id,
                        year,
                        duration
                        FROM staging_songs
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, 
                        latitude, longitude)
                        SELECT DISTINCT artist_id,
                        artist_name AS name,
                        artist_location AS location,
                        artist_latitude AS latitude,
                        artist_longitude AS longitude
                        FROM staging_songs
""")



time_table_insert = ("""INSERT INTO time_of_use (start_time, hour, day, week, month, 
                        year, weekday)
                        SELECT DISTINCT(DATEADD(s, ts/1000, '19700101')) AS start_time,
                        EXTRACT(hour from start_time) AS hour,
                        EXTRACT(day from start_time) AS day,
                        EXTRACT(week from start_time) AS week,
                        EXTRACT(month from start_time) AS month,
                        EXTRACT(year from start_time) AS year,
                        EXTRACT(weekday from start_time) AS weekday
                        FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
