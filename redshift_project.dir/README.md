# Project: Data Warehouse with AWS Redshift

### Project Description
Sparkify is a simulated on-line song service, which generates log files documenting users' activities on the website. The goal of this project is to put the dataset into a data warehouse hosted on AWS redshift, together with an even larger song database with song titles, artist and etc.

### Table Design
For the staging tables: stage_logdata and stage_songdata, since we will do a join on song title to produce the fact_songplays table, we use song title as distkey.

For analytic tables which use star schema, we use artist_id as distkey, start_time (if available) as sortkey. *NOTES*:

### Files
redshift_project.cfg: contains parameters to connect to data warehouse
create_tables.py: drop tables if they exist and create tables (see below)
etl.py: performs ETL tasks, which load the original JSON file into staging tables, then transforms data from staging tables to analytic tables.
sql_queries: contain all the SQL queries used in the python codes

### How to Run
