# Project 5: Data Pipelines with Airflow

## Summary
* [Introduction](#Introduction)
* [ELT Process](#ELT-Process)
* [Sources](#Sources)
* [Destinations](#Destinations)
* [Project Structure](#Project-Structure)
* [Data Quality Checks](#Data-Quality-Checks)

--------------------------------------------

### Introduction
A music streaming company, Sparkify, has decided that it is time to introduce more 
automation and monitoring to their data warehouse ETL pipelines and come to the 
conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade 
data pipelines that are dynamic and built from reusable tasks, can be monitored, and 
allow easy backfills. They have also noted that the data quality plays a big part 
when analyses are executed on top the data warehouse and want to run tests against 
their datasets after the ETL steps have been executed to catch any discrepancies in 
the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse 
in Amazon Redshift. The source datasets consist of CSV logs that tell about user 
activity in the application and JSON metadata about the songs the users listen to.

### ELT Process

The tool used for scheduling and orchestrationg ELT is Apache Airflow.

'Airflow is a platform to programmatically author, schedule and monitor workflows.'

Source: [Apache Foundation](https://airflow.apache.org/)

The schema of the ELT is this DAG:

![DAG](./images/dag.png)

### Sources

The sources are the same than previous projects:

* `Log data: s3://udacity-dend/log_data`
* `Song data: s3://udacity-dend/song_data`

### Destinations

Data is inserted into Amazon Redshift Cluster. The goal is populate an star schema:

* Fact Table:

    * `songplays` 

* Dimension Tables

    * `users - users in the app`
    * `songs - songs in music database`
    * `artists - artists in music database`
    * `time - timestamps of records in songplays broken down into specific units`

By the way we need two staging tables:

* `Stage_events`
* `Stage_songs`

##### Prerequisite   

Tables must be created in Redshift before executing the DAG workflow. The create tables script can be found in:

`create_tables.sql`



### Project Structure

* /
    * `create_tables.sql` - Contains the DDL for all tables used in this projecs
* dags
    * `udac_example_dag.py` - The DAG configuration file to run in Airflow
* plugins
    * operators
        * `stage_redshift.py` - Operator to read files from S3 and load into Redshift staging tables
        * `load_fact.py` - Operator to load the fact table in Redshift
        * `load_dimension.py` - Operator to read from staging tables and load the dimension tables in Redshift
        * `data_quality.py` - Operator for data quality checking
    * helpers
        * `sql_queries` - Redshift statements used in the DAG

### Data Quality Checks

In order to ensure the tables were loaded, 
a data quality checking is performed to count the total records each table has. 
If a table has no rows then the workflow will fail and throw an error message.