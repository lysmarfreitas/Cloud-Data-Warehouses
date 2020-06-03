import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS _staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS _staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLE QUERIES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist          VARCHAR,
    auth            VARCHAR, 
    firstName       VARCHAR,
    gender          VARCHAR,   
    itemInSession   INTEGER,
    lastName        VARCHAR,
    length          FLOAT,
    level           VARCHAR, 
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    BIGINT,
    sessionId       INTEGER,
    song            VARCHAR,
    status          INTEGER,
    ts              TIMESTAMP,
    userAgent       VARCHAR,
    userId          INTEGER
);


""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    song_id            VARCHAR,
    num_songs          INTEGER,
    title              VARCHAR,
    artist_name        VARCHAR,
    artist_latitude    FLOAT,
    year               INTEGER,
    duration           FLOAT,
    artist_id          VARCHAR,
    artist_longitude   FLOAT,
    artist_location    VARCHAR    
);

""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id        INTEGER IDENTITY(0,1) PRIMARY KEY, 
    start_time         TIMESTAMP, 
    user_id            INTEGER NOT NULL, 
    level              VARCHAR,
    song_id            VARCHAR, 
    artist_id          VARCHAR, 
    session_id         INTERGER, 
    location           VARCHAR, 
    user_agent         VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id           INTEGER PRIMARY KEY, 
    firstName         VARCHAR, 
    lastName          VARCHAR,
    gender            VARCHAR, 
    level             VARCHAR
);
""")


song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id          VARCHAR PRIMARY KEY, 
    title            VARCHAR, 
    artist_id        VARCHAR, 
    year             INTEGER, 
    duration         FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id         VARCHAR PRIMARY KEY, 
    artist_name       VARCHAR, 
    artist_location   VARCHAR, 
    artist_latitude   FLOAT8, 
    artist_longitude  FLOAT8
);
""")


time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time      TIMESTAMP PRIMARY KEY, 
    hour            SMALLINT, 
    day             SMALLINT, 
    week            SMALLINT, 
    month           SMALLINT, 
    year            SMALLINT, 
    weekday         SMALLINT
);
""")


# COPY TABLE QUERIES / STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    compupdate off region 'us-west-2' FORMAT AS JSON {}
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
    """).format(config['S3'].get('LOG_DATA'),
                config['IAM_ROLE'].get('ARN').strip("'"),
                config['S3'].get('LOG_JSONPATH'))


staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    compupdate off region 'us-west-2'
    FORMAT AS JSON 'auto'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
    """).format(config['S3'].get('SONG_DATA'),
                config['IAM_ROLE'].get('ARN').strip("'"))


# INSERT TABLE QUERIES / ANALYTICS TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
                se.userId as user_id,
                se.level as level,
                ss.song_id as song_id,
                ss.artist_id as artist_id,
                se.sessionId as session_id,
                se.location as location,
                se.userAgent as user_agent
FROM staging_events se
JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name;

""")

user_table_insert = ("""

INSERT INTO users(user_id, firstName, lastName, gender, level)
SELECT DISTINCT se.userId as user_id,
                se.FirstName as firstName,
                se.lastName as lastName,
                se.gender as gender,
                se.level as level

FROM staging_events se
WHERE userId IS NOT NULL;
""")

song_table_insert = ("""

INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id as song_id,
                title as title,
                artist_id as artist_id,
                year as year,
                duration as duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""

INSERT INTO artists(artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
SELECT DISTINCT artist_id as artist_id,
                artist_name as artist_name,
                artist_location as artist_location,
                artist_latitude as artist_latitude,
                artist_longitude as artist_longitude

FROM staging_songs
where artist_id IS NOT NULL;
""")

time_table_insert = ("""

INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts,
                EXTRACT(hour from ts),
                EXTRACT(day from ts),
                EXTRACT(week from ts),
                EXTRACT(month from ts),
                EXTRACT(year from ts),
                EXTRACT(weekday from ts)
FROM staging_events
WHERE ts IS NOT NULL;

""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
