# Project 3: Create a Data Warehouse with AWS Redshift

## Summary
* [Introduction](#Introduction)
* [Data Warehouse Schema Definition](#Data-Warehouse-Schema-Definition)
* [ETL process](#ETL-process)
* [Project structure](#Project-structure)

--------------------------------------------

### Introduction


A music streaming startup, Sparkify, has grown their user base and song database and want 
to move their processes and data onto the cloud. Their data resides in S3, in a directory 
of JSON logs on user activity on the app, as well as a directory with JSON metadata 
on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts 
their data from S3, stages them in Redshift, and transforms data into a set of 
dimensional tables for their analytics team to continue finding insights in what songs 
their users are listening to. You'll be able to test your database and ETL pipeline 
by running queries given to you by the analytics team from Sparkify and compare your 
results with their expected results

In this project we are going to use two Amazon Web Services resources:
* [S3](https://aws.amazon.com/en/s3/)
* [AWS Redshift](https://aws.amazon.com/en/redshift/)

The data sources to ingest into data warehouse are provided by two public S3 buckets:

1. Songs bucket (s3://udacity-dend/song_data), contains info about songs and artists. 
All files are in the same directory.
2. Event bucket (s3://udacity-dend/log_data), contains info about actions done by users, what song are listening, ... 
We have differents directories so we need a descriptor file (also a JSON) in order to extract
data from the folders by path. We used a descriptor file (s3://udacity-dend/log_json_path.json) because we 
don't have a common prefix on folders

The objects contained in both buckets are JSON files. The song bucket has all
the files under the same directory but <br> the event ones don't,
so we need a descriptor file (also a JSON) in order to extract
data from the folders by path. We used a descriptor file because we don't 
have a common prefix on folders

We need to ingest this data into AWS Redshift using COPY command. This command get JSON files
from buckets and copy them into staging tables inside AWS Redshift.

<b>Log Dataset structure:</b>
![Log Dataset](./images/log_dataset.jpg)

<b>Song dataset structure:</b>
~~~~
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null
, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", 
"title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
~~~~
--------------------------------------------

### Data Warehouse Schema Definition
This is the schema of the database

#### Staging tables

##### TABLE staging_songs

| COLUMN | TYPE | FEATURES |
| ------ | ---- | ------- |
|num_songs| int| |
|artist_id| varchar| |
|artist_latitude | decimal | |
|artist_longitude| decimal| |
|artist_location| varchar| |
|artist_name | varchar | |
|song_id| varchar| |
|title| varchar| |
|duration | decimal | |
|year | int | |


##### TABLE staging_events

| COLUMN | TYPE | FEATURES |
| ------ | ---- | ------- |
|artist| varchar| |
|auth| varchar| |
|firstName | varchar | |
|gender| varchar| |
|itemInSession | int| |
|lastName | varchar | |
|length| decimal| |
|level| varchar| |
|location | varchar| |
|method | varchar| |
|page | varchar | |
|registration| varchar| |
|sessionId| int| |
|song | varchar| |
|status| int| |
|ts| timestamp| |
|userAgent| varchar| |
|userId| int| |


#### Dimension tables

##### TABLE users

| COLUMN | TYPE | FEATURES |
| ------ | ---- | ------- |
|user_id| int| distkey, PRIMARY KEY |
|first_name| varchar| |
|last_name | varchar | |
|gender| varchar| |
|level| varchar| |

##### TABLE songs

| COLUMN | TYPE | FEATURES |
| ------ | ---- | ------- |
|song_id| varchar| sortkey, PRIMARY KEY |
|title| varchar| NOT NULL |
|artist_id | varchar | NOT NULL|
|duration| decimal| |

##### TABLE artists

| COLUMN | TYPE | FEATURES |
| ------ | ---- | ------- |
|artist_id| varchar| sortkey, PRIMARY KEY |
|name| varchar| NOT NULL |
|location | varchar | |
|latitude| decimal| |
|logitude| decimal| |


##### TABLE time

| COLUMN | TYPE | FEATURES |
| ------ | ---- | ------- |
|start_time| timestamp| sortkey, PRIMARY KEY |
|hour| int| |
|day| int| |
|week| int| |
|month| int| |
|year| int| |
|weekday| int| |

#### Fact table

##### TABLE songplays

| COLUMN | TYPE | FEATURES |
| ------ | ---- | ------- |
|songplay_id| int| IDENTITY (0,1), PRIMARY KEY |
|start_time| timestamp| REFERENCES  time(start_time)    sortkey|
|user_id | int | REFERENCES  users(user_id) distkey|
|level| varchar| |
|song_id| varchar| REFERENCES  songs(song_id)|
|artist_id | varchar | REFERENCES  artists(artist_id)|
|session_id| int| NOT NULL|
|location| varchar| |
|user_agent| varchar| |

--------------------------------------------

### ETL process

All the transformations logic (ETL) is done in SQL inside Redshift. 

There are 2 main steps:

1. Ingest data from s3 public buckets into staging tables:
2. Insert record into a star schema from staging tables

#### Insert data into staging tables

<b>Insert into staging events:</b>
~~~~
 staging_events_copy = ("""
    COPY staging_events 
        FROM {} 
        iam_role {} 
        region {}
        FORMAT AS JSON {} 
        timeformat 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(LOG_DATA, ARN, REGION, LOG_JSON_PATH)
~~~~

<b>Insert into staging songs:</b>
~~~~
staging_songs_copy = ("""
    COPY staging_songs 
        FROM {}
        iam_role {}
        region {}
        FORMAT AS JSON 'auto' 
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, ARN, REGION)
~~~~

#### Insert data into star schema from staging tables

<b>Insert Dimensions:</b>
~~~~

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT se.userId, 
                        se.firstName, 
                        se.lastName, 
                        se.gender, 
                        se.level
        FROM staging_events se
        WHERE se.userId IS NOT NULL;
""")


song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration) 
        SELECT DISTINCT ss.song_id, 
                        ss.title, 
                        ss.artist_id, 
                        ss.year, 
                        ss.duration
        FROM staging_songs ss
        WHERE ss.song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, logitude)
        SELECT DISTINCT ss.artist_id, 
                        ss.artist_name, 
                        ss.artist_location,
                        ss.artist_latitude,
                        ss.artist_longitude
        FROM staging_songs ss
        WHERE ss.artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT  se.ts,
                        EXTRACT(hour from se.ts),
                        EXTRACT(day from se.ts),
                        EXTRACT(week from se.ts),
                        EXTRACT(month from se.ts),
                        EXTRACT(year from se.ts),
                        EXTRACT(weekday from se.ts)
        FROM staging_events se
        WHERE se.page = 'NextSong';
""")
~~~~

<b>Insert Facts table:</b>
~~~~
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
        SELECT DISTINCT se.ts, 
                        se.userId, 
                        se.level, 
                        ss.song_id, 
                        ss.artist_id, 
                        se.sessionId, 
                        se.location, 
                        se.userAgent
        FROM staging_events se 
        INNER JOIN staging_songs ss 
            ON se.song = ss.title AND se.artist = ss.artist_name
        WHERE se.page = 'NextSong';
""")
~~~~

--------------------------------------------

#### Project structure

The structure is:

* <b> create_tables.py </b> - This script will drop old tables (if exist) ad re-create new tables
* <b> etl.py </b> - This script orchestrate ETL.
* <b> sql_queries.py </b> - This is the ETL. All the transformatios in SQL are done here.
* <b> /img </b> - Directory with images that are used in this markdown document

We need an extra file with the credentials an information about AWS resources named <b>dhw.cfg</b>


