class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    users_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

# CREATE TABLES
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
