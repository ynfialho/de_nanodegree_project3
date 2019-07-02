import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOGGER_DATA = config.get("S3", "LOG_DATA")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE_NAME = config.get("IAM_ROLE", "ARN")
LOGGER_JSONPATH = config.get("S3", "LOG_JSONPATH")

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS stg_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_songs"
songplay_table_drop = "DROP TABLE IF EXISTS ft_songplays"
user_table_drop = "DROP TABLE IF EXISTS dim_users"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES
staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS stg_events (
        event_id INT IDENTITY(0,1),
        artist VARCHAR(400) ENCODE ZSTD,
        auth VARCHAR(400) ENCODE ZSTD,
        firstName VARCHAR(400) ENCODE ZSTD,
        gender VARCHAR(1),
        itemInSession INT,
        lastName VARCHAR(400) ENCODE ZSTD,
        length FLOAT8, 
        level VARCHAR(50) ENCODE ZSTD,
        location VARCHAR(400) ENCODE ZSTD,	
        method VARCHAR(25) ENCODE ZSTD,
        page VARCHAR(35) ENCODE ZSTD,	
        registration BIGINT,	
        session_id BIGINT,
        song VARCHAR(400) ENCODE ZSTD,
        status INT,	
        ts VARCHAR(50) ENCODE ZSTD,
        user_agent TEXT ENCODE ZSTD,	
        user_id INT,
        PRIMARY KEY(event_id)) """)

staging_songs_table_create = ("""
     CREATE TABLE IF NOT EXISTS stg_songs (
        num_songs INTEGER,
        artist_id VARCHAR(400) ENCODE ZSTD,
        artist_latitude FLOAT8,
        artist_longitude FLOAT8,
        artist_location VARCHAR(400) ENCODE ZSTD,
        artist_name VARCHAR(400) ENCODE ZSTD,
        song_id VARCHAR(400) ENCODE ZSTD,
        title VARCHAR(400) ENCODE ZSTD,
        duration FLOAT8,
        year INT,
        PRIMARY KEY(song_id)) """)

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS ft_songplays (
        songplay_id BIGINT IDENTITY(0,1),
        start_time TIMESTAMP NOT NULL,   
        user_id INTEGER,
        level VARCHAR(50) ENCODE ZSTD,
        song_id VARCHAR(400), 
        artist_id VARCHAR(400) ENCODE ZSTD,
        session_id INTEGER,
        location VARCHAR(400) ENCODE ZSTD,
        user_agent TEXT ENCODE ZSTD,
        PRIMARY KEY(songplay_id),
        FOREIGN KEY(start_time) REFERENCES dim_time(start_time),
        FOREIGN KEY(user_id) REFERENCES dim_users(user_id),
        FOREIGN KEY(song_id) REFERENCES dim_songs(song_id),
        FOREIGN KEY(artist_id) REFERENCES dim_artists(artist_id))
    DISTKEY(song_id)
    SORTKEY(song_id, start_time);
""")

user_table_create = ("""
    CREATE TABLE dim_users(
        user_id VARCHAR NOT NULL,
        first_name VARCHAR(400) NOT NULL ENCODE ZSTD,
        last_name VARCHAR(400) NOT NULL ENCODE ZSTD,
        gender VARCHAR(1),
        level VARCHAR(50) ENCODE ZSTD,
        PRIMARY KEY (user_id)
    )
""")

song_table_create = ("""
    CREATE TABLE dim_songs(
        song_id VARCHAR(400) NOT NULL ENCODE ZSTD,
        title VARCHAR(400) NOT NULL ENCODE ZSTD,
        artist_id VARCHAR(400) ENCODE ZSTD,
        year INTEGER,
        duration FLOAT8,
        PRIMARY KEY (song_id))
    DISTKEY(song_id);
""")

artist_table_create = ("""
    CREATE TABLE dim_artists(
        artist_id VARCHAR(400) NOT NULL ENCODE ZSTD,
        name VARCHAR(400) NOT NULL ENCODE ZSTD,
        location VARCHAR(400) ENCODE ZSTD,
        latitude FLOAT8,
        longitude FLOAT8,
        PRIMARY KEY (artist_id) )
    DISTSTYLE ALL;
""")

time_table_create = ("""
    CREATE TABLE dim_time(
        start_time TIMESTAMP,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday INTEGER,
        PRIMARY KEY (start_time))
    DISTSTYLE AUTO;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY stg_events 
    FROM {}
    IAM_ROLE {}
    JSON {}
    REGION 'us-west-2'
    STATUPDATE OFF
    COMPUPDATE OFF
""").format(LOGGER_DATA, IAM_ROLE_NAME, LOGGER_JSONPATH)

staging_songs_copy = ("""
    COPY stg_songs 
    FROM {} 
    IAM_ROLE {} 
    JSON 'auto' 
    REGION 'us-west-2'
    STATUPDATE OFF
    COMPUPDATE OFF
""").format(SONG_DATA, IAM_ROLE_NAME)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO ft_songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT 
        TIMESTAMP 'epoch' + e.ts::INT8/1000 * INTERVAL '1 second' AS start_time,
        e.user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.session_id,
        e.location,
        e.user_agent    
    FROM stg_events e
    LEFT JOIN stg_songs s
        ON e.song = s.title
        AND e.artist = s.artist_name
    WHERE page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO dim_users (user_id, first_name, last_name, gender, level)
    SELECT sub.user_id,
        sub.firstName,
        sub.lastName,
        sub.gender,
        sub.level
    FROM (select  sub.user_id,
            firstName,
            lastName,
            gender,
            level,
            row_number() over (partition by user_id order by ts desc) AS row_num
        from stg_events) sub
    WHERE 
        sub.user_id IS NOT NULL AND 
        sub.row_num = 1
""")

song_table_insert = ("""
    INSERT INTO dim_songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id,
        title,
        artist_id,
        year,    
        duration
    FROM stg_songs;
""")

artist_table_insert = ("""
    INSERT INTO dim_artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id,
        artist_name,
        artist_location,
        artist_latitude,    
        artist_longitude
    FROM stg_songs;
""")

time_table_insert = ("""
    INSERT INTO dim_time(start_time, hour, day, week, month, year, weekday)
    SELECT start_time,
        EXTRACT(hour from start_time) AS hour,
        EXTRACT(day from start_time) AS day,
        EXTRACT(week from start_time) AS week,
        EXTRACT(month from start_time) AS month,
        EXTRACT(year from start_time) AS year,
        EXTRACT(dow from start_time) AS weekday
    FROM ft_songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
