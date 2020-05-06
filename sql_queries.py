import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_IAM_ROLE = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
  CREATE TABLE staging_events (
    artist VARCHAR, 
    auth VARCHAR,
    firstName VARCHAR,
    gender CHAR(1),
    itemInSession SMALLINT NOT NULL,
    lastName VARCHAR,
    length REAL,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR NOT NULL,
    page VARCHAR NOT NULL,
    registration BIGINT,
    sessionId INT NOT NULL,
    song VARCHAR,
    status SMALLINT NOT NULL,
    ts BIGINT NOT NULL,
    userAgent VARCHAR,
    userId INT
  )
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    num_songs SMALLINT,
    artist_id VARCHAR,
    artist_latitude REAL,
    artist_longitude REAL,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration REAL,
    year SMALLINT
)
""")

songplay_table_create = ("""
CREATE TABLE songplay (
    songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
    start_time INT NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR,
    song_id VARCHAR NOT NULL DISTKEY SORTKEY,
    artist_id INT NOT NULL,
    session_id INT,
    location VARCHAR NULL,
    user_agent VARCHAR NULL
    );
""")

user_table_create = ("""
CREATE TABLE users (
    user_id INT PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender CHAR(1),
    level VARCHAR
    ) diststyle all;
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id VARCHAR PRIMARY KEY DISTKEY,
    title VARCHAR NOT NULL,
    artist_id INT NOT NULL,
    year SMALLINT NOT NULL,
    duration REAL
    ); 
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude REAL,
    longitude REAL
    ) diststyle all;
""")

time_table_create = ("""
CREATE TABLE time (
    start_time TIMESTAMP PRIMARY KEY,
    hour SMALLINT NOT NULL,
    day SMALLINT NOT NULL,
    week SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    year SMALLINT NOT NULL,
    weekday SMALLINT NOT NULL
    ) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM 's3://udacity-dend/log_data'
IAM_ROLE {}
REGION 'us-west-2' compupdate off 
JSON 's3://udacity-dend/log_json_path.json';
""").format(DWH_IAM_ROLE)

staging_songs_copy = ("""
copy staging_songs 
FROM 's3://udacity-dend/song_data'
IAM_ROLE {}
REGION 'us-west-2' compupdate off 
JSON 'auto';
""").format(DWH_IAM_ROLE)

# FINAL TABLES
# Assuming no two songs are of same title and duration for song_id lookup
# Assuming combo of artist name and location make unique artist_id 
songplay_table_insert = ("""
INSERT INTO songplay (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
SELECT 
	  TIMESTAMP 'epoch' + (ts/1000) * interval '1 second',
    userid,
    level,
    (SELECT song_id FROM songs WHERE song.title = se.song AND song.duration = se.length),
    (SELECT artist_id FROM artists WHERE artists.name = se.artist and artists.location = se.artist_location),
    sessionid,
    location,
    useragent
FROM staging_events se
WHERE page = 'NextSong'
""")

# Design decision: level is equal to the last event record by timestamp for each user
# Adapted from https://stackoverflow.com/questions/6841605/get-top-1-row-of-each-group
user_table_insert = ("""
WITH last_level AS
(
  SELECT *, 
         ROW_NUMBER() OVER (PARTITION BY userid ORDER BY ts DESC) as rn
  FROM staging_events
)
INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level
)
SELECT DISTINCT userid,
        firstName,
        lastName,
        gender,
        level
FROM last_level
WHERE rn = 1
""")

song_table_insert = ("""
INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration 
)
SELECT DISTINCT
                song_id,
                title,
                artist_id,
                year,
                duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (
          artist_id,
          name
          location,
          latitude,
          longitude
)
SELECT DISTINCT
                artist_id,
                artist_name,
                artist_location,
                artist_latitude,
                artist_longitude
FROM staging_songs
""")

# Source: https://knowledge.udacity.com/questions/74200
time_table_insert = ("""
INSERT INTO time (
       start_time,
       hour,
       day,
       week,
       month,
       year,
       weekday
)
SELECT ts.start_time,
       EXTRACT(HOUR FROM ts.start_time),
       EXTRACT(DAY FROM ts.start_time),
       EXTRACT(WEEK FROM ts.start_time),
       EXTRACT(MONTH FROM ts.start_time),
       EXTRACT(YEAR FROM ts.start_time),
       EXTRACT(WEEKDAY FROM ts.start_time) AS weekday
FROM (
	SELECT 
	  TIMESTAMP 'epoch' + (ts/1000) * interval '1 second' as start_time 
	FROM staging_events
) ts
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
