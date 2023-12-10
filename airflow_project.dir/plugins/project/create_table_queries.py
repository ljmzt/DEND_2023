class CreateTable:
    create = '''
        CREATE TABLE IF NOT EXISTS public.staging_events (
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

        CREATE TABLE IF NOT EXISTS public.staging_songs (
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

        CREATE TABLE IF NOT EXISTS public.fact_songplays (
          songplay_id VARCHAR(32),
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

        CREATE TABLE IF NOT EXISTS public.dim_users (
          user_id INTEGER,
          first_name TEXT,
          last_name TEXT,
          gender CHAR(1),
          level VARCHAR(20)
        ) DISTSTYLE ALL;

        CREATE TABLE IF NOT EXISTS public.dim_songs (
          song_id CHAR(18),
          title VARCHAR(1024),
          artist_id CHAR(18) DISTKEY,
          year SMALLINT,
          duration REAL
        );

        CREATE TABLE IF NOT EXISTS public.dim_artists (
          artist_id CHAR(18) DISTKEY,
          name VARCHAR(1024),
          location VARCHAR(1024),
          latitude REAL,
          longitude REAL
        );

        CREATE TABLE IF NOT EXISTS public.dim_times (
          hour_stamp INTEGER PRIMARY KEY,
          hour SMALLINT,
          day SMALLINT,
          week SMALLINT,
          month SMALLINT,
          year SMALLINT,
          weekday SMALLINT
        ) DISTSTYLE ALL;
'''

