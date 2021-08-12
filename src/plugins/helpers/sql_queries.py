staging_songs_table_create = ("""
                              CREATE TABLE IF NOT EXISTS staging_songs_table ( \
                                artist_id            varchar,  \
                                artist_latitude      decimal,  \
                                artist_location      varchar,  \
                                artist_longitude     decimal, \
                                artist_name          varchar,  \
                                duration             float8,  \
                                num_songs            int,  \
                                song_id              varchar,  \
                                title                varchar,  \
                                year                 int
                                );
                              """)

staging_events_table_create = ("""
                              CREATE TABLE IF NOT EXISTS staging_events_table ( \
                                artist              varchar, \
                                auth                varchar, \
                                firstName           varchar, \
                                gender              varchar, \
                                itemInSession       int, \
                                lastName            varchar,  \
                                length              float8,  \
                                level               varchar, \
                                location            varchar,  \
                                method              varchar, \
                                page                varchar,  \
                                registration        varchar, \
                                sessionId           int, \
                                song                varchar, \
                                status              int,  \
                                ts                  bigint, \
                                userAgent           varchar, \
                                userId              int);
                              """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay ( \
                            songplay_id             int IDENTITY (0, 1) PRIMARY KEY, \
                            start_time              TIMESTAMP NOT NULL, \
                            user_id                 int NOT NULL, \
                            level                   varchar NOT NULL, \
                            song_id                 varchar ,  \
                            artist_id               varchar,  \
                            session_id              varchar NOT NULL, \
                            location                varchar, \
                            user_agent              varchar)""")

users_table_create = ("""CREATE TABLE IF NOT EXISTS users ( \
                            user_id             int PRIMARY KEY,  \
                            first_name          varchar, \
                            last_name           varchar, \
                            gender              varchar, \
                            level               varchar NOT NULL)""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song ( \
                        song_id             varchar PRIMARY KEY, \
                        title               varchar NOT NULL, \
                        artist_id           varchar NOT NULL, \
                        year                int, \
                        duration            numeric)""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist ( \
                            artist_id           varchar PRIMARY KEY, \
                            name                varchar NOT NULL, \
                            location            varchar, \
                            latitude            decimal, \
                            longitude           decimal)""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time ( \
                            start_time          timestamp PRIMARY KEY, \
                            hour                int NOT NULL, \
                            day                 int NOT NULL, \
                            week                int NOT NULL, \
                            month               int NOT NULL, \
                            year                int NOT NULL, \
                            weekday             int NOT NULL)""")

# STAGING TABLES
copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{{}}'
            SECRET_ACCESS_KEY '{{}}'
            IGNOREHEADER 1
            DELIMITER ','
           """


staging_events_copy = (f"""
                         copy staging_events_table from {config.get('S3','LOG_DATA')}
                         credentials 'aws_iam_role={DWH_ROLE_ARN}'
                         JSON {config.get('S3','LOG_JSONPATH')} 
                         compupdate off region 'us-west-2';
                         """)

staging_songs_copy = (f"""
                       copy staging_songs_table from {config.get('S3','SONG_DATA')}
                       credentials 'aws_iam_role={DWH_ROLE_ARN}'
                       JSON 'auto' 
                       compupdate off region 'us-west-2';
                       """)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay (start_time, user_id, level, \
                                                    song_id, artist_id, session_id, \
                                                    location, user_agent) \
                            SELECT 
                                (TIMESTAMP 'epoch' + se.ts/1000*INTERVAL '1 second') AS start_time, \
                                userId, level, \
                                song_id, artist_id, \
                                sessionId, location, userAgent
                            FROM staging_events_table AS se 
                                LEFT JOIN staging_songs_table AS ss
                                ON se.artist = ss.artist_name 
                                AND se.song = ss.title
                                AND se.length = ss.duration
                            WHERE se.sessionId NOT IN (SELECT session_id FROM songplay)
                                AND se.page = 'NextSong'
                                AND se.userId IS NOT NULL
                                AND ss.artist_id IS NOT NULL
                                AND se.level IS NOT NULL
                                AND se.sessionId IS NOT NULL
                                AND ss.song_id IS NOT NULL
                                AND se.ts IS NOT NULL; """)

users_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                         SELECT  DISTINCT userId, firstName, lastName, gender, level
                         FROM staging_events_table 
                        WHERE userId NOT IN (SELECT DISTINCT user_id FROM users) 
                            AND userId IS NOT NULL
                            AND level IS NOT NULL;""")

song_table_insert = ("""INSERT INTO song (song_id, title, artist_id, year, duration) \
                        SELECT DISTINCT song_id, title, artist_id, year, duration
                        FROM staging_songs_table
                        WHERE song_id NOT IN (SELECT DISTINCT song_id FROM song)
                            AND  title IS NOT NULL
                            AND artist_id IS NOT NULL;""")

artist_table_insert = ("""INSERT INTO artist (artist_id, name, location, latitude, longitude) \
                          SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                          FROM staging_songs_table
                          WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artist)
                            AND artist_id IS NOT NULL
                            AND artist_name is NOT NULL;""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
                        SELECT DISTINCT start_time,
                            EXTRACT(HOUR FROM start_time),
                            EXTRACT(DAY FROM start_time),
                            EXTRACT(WEEK FROM start_time),
                            EXTRACT(MONTH FROM start_time),
                            EXTRACT(YEAR FROM start_time),
                            EXTRACT(WEEKDAY FROM start_time)
                        FROM (SELECT
                                TIMESTAMP 'epoch' + ts/1000*INTERVAL '1 second' AS start_time
                                FROM staging_events_table
                                WHERE ts IS NOT NULL) AS sb
                        WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time);""")
