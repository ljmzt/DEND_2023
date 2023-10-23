import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('redshift_project.cfg')

iam_role_arn = config.get('IAM_ROLE', 'ARN') 
log_data = config.get('S3', 'LOG_DATA')
log_jsonpath = config.get('S3', 'LOG_JSONPATH')
song_data = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_logdata"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_songdata"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays"
user_table_drop = "DROP TABLE IF EXISTS dim_users"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_times"

# CREATE TABLES

# for staging tables, we need to join on song title for the fact_songplay, so we set song title as distkey
# in redshift, text=varchar(256), so artist name and location were wrong in an ealier attempt!!
staging_events_table_create= ("""
  CREATE TABLE stage_logdata (
  artist VARCHAR(1024),
  auth VARCHAR(20),
  firstName TEXT,
  gender CHAR(1),
  iteminSession INTEGER,
  lastName TEXT,
  length REAL,
  level VARCHAR(20),
  location VARCHAR(1024),
  method VARCHAR(20),
  page VARCHAR(100),
  registration REAL,
  sessionId INTEGER,
  song VARCHAR(1024) DISTKEY,
  status INTEGER,
  ts BIGINT,
  userAgent TEXT,
  userId INT
);
""")

staging_songs_table_create = ("""
  CREATE TABLE stage_songdata (
  num_songs SMALLINT,
  artist_id CHAR(18),
  artist_latitude REAL,
  artist_longitude REAL,
  artist_location VARCHAR(1024),
  artist_name VARCHAR(1024),
  song_id CHAR(18),
  title VARCHAR(1024) DISTKEY,
  duration REAL,
  year SMALLINT
);
""")

# for the actual tables for analysis, all tables has artist_id, so we use that at distkey
# timestamp can be used as sortkey
# small tables like dim_users and dim_times can use diststyle all
songplay_table_create = ("""
  CREATE TABLE fact_songplays (
  songplay_id BIGINT IDENTITY(0,1),
  start_time timestamp SORTKEY,
  hour_stamp INTEGER,
  user_id INTEGER,
  level VARCHAR(20),
  song_id CHAR(18),
  artist_id CHAR(18) DISTKEY,
  session_id INTEGER,
  location VARCHAR(1024),
  user_agent TEXT
);
""")

user_table_create = ("""
  CREATE TABLE dim_users (
  user_id INTEGER,
  first_name TEXT,
  last_name TEXT,
  gender CHAR(1),
  level VARCHAR(20)
) DISTSTYLE ALL;
""")

song_table_create = ("""
  CREATE TABLE dim_songs (
  song_id CHAR(18),
  title VARCHAR(1024),
  artist_id CHAR(18) DISTKEY,
  year SMALLINT,
  duration REAL
);
""")

artist_table_create = ("""
  CREATE TABLE dim_artists (
  artist_id CHAR(18) DISTKEY,
  name VARCHAR(1024),
  location VARCHAR(1024),
  latitude REAL,
  longitude REAL
);
""")

time_table_create = ("""
  CREATE TABLE dim_times (
  hour_stamp INTEGER PRIMARY KEY,
  hour SMALLINT,
  day SMALLINT,
  week SMALLINT,
  month SMALLINT,
  year SMALLINT,
  weekday SMALLINT
) DISTSTYLE ALL;
""")

# STAGING TABLES
# use log_jsonpath to help better parse the json file
staging_events_copy = ("""
  COPY stage_logdata
  FROM {log_data}
  CREDENTIALS 'aws_iam_role={iam_role_arn}'
  REGION 'us-west-2'
  FORMAT AS JSON {log_jsonpath}
""").format(log_data=log_data, iam_role_arn=iam_role_arn, log_jsonpath=log_jsonpath)

staging_songs_copy = ("""
  COPY stage_songdata
  FROM {song_data}
  CREDENTIALS 'aws_iam_role={iam_role_arn}'
  REGION 'us-west-2'
  FORMAT AS JSON 'auto'
""").format(song_data=song_data, iam_role_arn=iam_role_arn)

# FINAL TABLES = dim tables + fact tables
# dws doesn't have to_timestamp function, so need to do this epoch + xx interval thing
songplay_table_insert = ("""
INSERT INTO fact_songplays (start_time, hour_stamp, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
  timestamp 'epoch' + l.ts/1000 * interval '1 second',
  (l.ts/1000)::INTEGER / 3600,
  l.userId,
  l.level,
  s.song_id,
  s.artist_id,
  l.sessionId,
  l.location,
  l.userAgent
FROM stage_logdata l
LEFT JOIN stage_songdata s ON l.page = 'NextSong' AND l.song = s.title;
""")

user_table_insert = ("""
INSERT INTO dim_users (user_id, first_name, last_name, gender, level)
SELECT
  DISTINCT userId,
  firstName,
  lastName,
  gender,
  level
FROM stage_logdata
""")

song_table_insert = ("""
INSERT INTO dim_songs (song_id, title, artist_id, year, duration)
SELECT
  song_id,
  title,
  artist_id,
  year,
  duration
FROM stage_songdata;
""")

artist_table_insert = ("""
INSERT INTO dim_artists (artist_id, name, location, latitude, longitude)
SELECT 
  DISTINCT artist_id,
  artist_name,
  artist_location,
  artist_latitude,
  artist_longitude
FROM stage_songdata;
""")

time_table_insert = ("""
INSERT INTO dim_times (hour_stamp, hour, day, week, month, year, weekday)
SELECT
  DISTINCT hour_stamp,
  date_part(hour, ts)::SMALLINT,
  date_part(day, ts)::SMALLINT,
  date_part(week, ts)::SMALLINT,
  date_part(month, ts)::SMALLINT,
  date_part(year, ts)::SMALLINT,
  date_part(weekday, ts)::SMALLINT
FROM (
  SELECT
    (ts/1000)::INTEGER / 3600 AS hour_stamp,
    (timestamp 'epoch' + ts/1000 * interval '1 second')::TIMESTAMP AS ts 
  FROM stage_logdata
) tmp;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
