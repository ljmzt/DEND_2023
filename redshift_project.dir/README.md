# Project: Data Warehouse with AWS Redshift

### Project Description
Sparkify is a simulated on-line song service, which generates log files documenting users' activities on the website. The goal of this project is to put the dataset into a data warehouse hosted on AWS redshift, together with an even larger song database with song titles, artist and etc.

### Table Design

These are the project description about analytic tables:  
**Fact Table**   
1. songplays - records in event data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimension Tables**
2. users - users in the app   
user_id, first_name, last_name, gender, level
3. songs - songs in music database   
song_id, title, artist_id, year, duration
4. artists - artists in music database   
artist_id, name, location, latitude, longitude
5. time - timestamps of records in songplays broken down into specific units   
start_time, hour, day, week, month, year, weekday

For the staging tables: stage_logdata and stage_songdata, since we will do a join on song title to produce the fact_songplays table, we use song title as distkey.

For analytic tables which use star schema, we use artist_id as distkey, start_time (if available) as sortkey.  dim_users and dim_times use diststyle all.   
**NOTES**: The original project description asks for designing a dimension table with timestamp in seconds. I don't agree with this. The log files rarely contains two users doing something at exactly the same time, so this dimension table would be almost as large as the original stage_logdata table. Look deeper into that, the timestamp dimension table only cares about day, hour, day of week etc, so it could save a lot of space by defining timestamp in terms hours since the predefined epoch (1970-01-01) without losing anything.  

### Files
redshift_project.cfg: contains parameters to connect to data warehouse
create_tables.py: drop tables if they exist and create tables (see below)
etl.py: performs ETL tasks, which load the original JSON file into staging tables, then transforms data from staging tables to analytic tables.
sql_queries: contain all the SQL queries used in the python codes

### How to Run
