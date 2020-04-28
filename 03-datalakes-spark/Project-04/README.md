# Project 4: Create a Data Lake with Spark

## Summary
* [Introduction](#Introduction)
* [Partition parquet files](#Partition-parquet-files)
* [ETL process](#ETL-process)
* [Project structure](#Project-structure)
* [How to run](#How-to-run)


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

In this project, you'll apply what you've learned on Spark and data lakes to build an ETL 
pipeline for a data lake hosted on S3. To complete the project, you will need to load 
data from S3, process the data into analytics tables using Spark, and load them back 
into S3. You'll deploy this Spark process on a cluster using AWS.

The data sources to ingest into data lake are provided by two public S3 buckets:

1. Songs bucket (s3://udacity-dend/song_data), contains info about songs and artists. 
All files are in the same directory.
2. Event bucket (s3://udacity-dend/log_data), contains info about actions done by users, what song are listening, ... 

<b>Log Dataset structure:</b>
![Log Dataset](./images/log_dataset.png)

<b>Song dataset structure:</b>
~~~~
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null
, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", 
"title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
~~~~
--------------------------------------------

### Partition parquet files
This is the schema of the datalake

#### Dimension tables

##### TABLE users

~~~~
root
 |-- firstName: string (nullable = true)
 |-- lastName: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- level: string (nullable = true)
 |-- userId: string (nullable = true)
~~~~

##### TABLE songs

~~~~
root
 |-- song_id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- year: long (nullable = true)
 |-- duration: double (nullable = true)
~~~~
partitionBy("year", "artist_id")

##### TABLE artists

~~~~
root
 |-- artist_id: string (nullable = true)
 |-- artist_name: string (nullable = true)
 |-- artist_location: string (nullable = true)
 |-- artist_latitude: double (nullable = true)
 |-- artist_longitude: double (nullable = true)
~~~~

##### TABLE time

~~~~
root
 |-- start_time: timestamp (nullable = true)
 |-- hour: integer (nullable = true)
 |-- day: integer (nullable = true)
 |-- week: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- year: integer (nullable = true)
 |-- weekday: integer (nullable = true)
~~~~

partitionBy("year", "month")

#### Fact table

##### TABLE songplays

~~~~
root
 |-- start_time: timestamp (nullable = true)
 |-- userId: string (nullable = true)
 |-- level: string (nullable = true)
 |-- sessionId: long (nullable = true)
 |-- location: string (nullable = true)
 |-- userAgent: string (nullable = true)
 |-- song_id: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- songplay_id: long (nullable = false)
~~~~
partitionBy("year", "month")

--------------------------------------------

### ETL process

All the transformations logic (ETL) are done by Spark. 



--------------------------------------------

#### Project structure

The structure is:

* <b> etl.py </b> - This script orchestrate ETL.
* <b> /img </b> - Directory with images that are used in this markdown document

We need an extra file with the credentials an information about AWS resources named <b>dhw.cfg</b>

#### How to run

1. Replace AWS IAM Credentials in dl.cfg
2. If needed, modify input and output data paths in etl.py main function
3. In the terminal, run python etl.py
