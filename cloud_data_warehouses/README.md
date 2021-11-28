# Sparkify User Song Plays

An Amazon Web Services (AWS) Extract-Transform-Load (ETL) process that allows queries to answer analytical questions about user song plays.

Analytical questions to answer may include the following to help keep users engaged:

1. Trending songs/albums.
2. Top 10 daily songs/albums.
3. User birthdays.
4. Users with a long time period since last usage.
5. Song suggestions based on user listen history.
6. Geographical maps for advertising.

## Quick Start

Terminal commands are showing for a Windows command prompt.

1. Create/Activate Virtual Environment & Install Dependancies

    ``` cmd
    python -m venv env
    .\env\Scripts\activate
    pip install -r requirements.txt
    ```

## ETL Process Overview

1. Source Files into Staging Tables

    Source files resides in an [AWS S3 bucket](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend/) in JSON files.

    ``` file
    s3://udacity-dend/song_data
    s3://udacity-dend/log_data
    ```

    TODO: The data is first loaded into staging tables to leverage the database engine's ability to join data from files in both log_data and event_data.

    TODO: is parallel loading being peformed?
    TODO: how many clusters should be used?

2. Staging Table into Analytical Tables

    Data is first loaded

## Configuration

A configuration file named dwh.cfg is required in the base directory. It should contain the following fields.

```cfg
[CLUSTER]
HOST=
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_PORT=

[IAM_ROLE]
ARN=''

[S3]
LOG_DATA='s3://udacity-dend/log-data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song-data'
```

## Infastructure as Code to Create/Delete Redshift Cluster

Manual required initial setup.

1. Navigate to IAM / Access Management / Users / Add users
2. User name = user_song_plays_setup
3. Select AWS credential type = Access key - Programatic access
4. Next: Permissions
5. Attach existing policies directly
6. Select AdministratorAccess
7. Next: Tags
8. Next: Review
9. Create user
10. Copy the provided values to the AWS section of dwh.cfg

    ``` dwh.cfg
    [AWS][KEY] = Access key ID
    [AWS][SECRET] = Secret access key
    ```

Install dependancies.

``` cmd
pip install -r infrastructure-requirements.py
```

Run the script to create the Redshift cluster infrastructure.

``` cmd
infrastructure.py create
```

This will produce a terminal output similar to the following.

``` cmd
Creating Redshift IAM Role YYYY.
Attaching Redshift IAM Role YYYY policy for S3 read only access.
Creating Redshift cluster sparkify.
Creating database user_song_plays in Redshift cluster.
Checking cluster availability. Attempt 0/4.
Waiting 30 seconds before checking cluster availability.
Redshift cluster sparkify is available.
Opening Redshift cluster TCP port 5439 for external access.
Setting [IAM_ROLE][ARN]=arn:aws:iam::XXX:role/YYY in config file dwh.cfg.
Setting [CLUSTER][HOST]=ZZZ.us-west-2.redshift.amazonaws.com in config file dwh.cfg.
```

***Carefully*** run the script to delete the Redshift infrastructure. This will delete everything without creating a snapshot or backup.

``` cmd
infrastructure.py delete
```

This will produce a terminal output similar to the following.

``` cmd
Deleting Redshift cluster sparkify.
Checking cluster deletion. Attempt 0/4.
Waiting 30 seconds before checking cluster deletion.
Redshift cluster sparkify has been deleted/does not exist.
Detaching S3 Policy from IAM Role songplays_S3toRedshiftStaging.
Deleting IAM Role songplays_S3toRedshiftStaging.
```

## Create Tables

## ETL

{'artist_location': [{'err_reason': 'String length exceeds DDL length', 'filename': 's3://udacity-dend/song-data/A/Y/F/TRAYFUW128F428F618.json'}]}

## Create Redshift Cluster (optional)

Running `infrastructure.py create` automatically creates a Redshift cluster.

The terminal output below documents the steps performed while the cluster is created.

```cmd
Creating Redshift IAM Role songplays_S3toRedshiftStaging.
Attaching Redshift IAM Role songplays_S3toRedshiftStaging policy for S3 read only access.
Creating Redshift cluster sparkify.
Creating database user_song_plays in Redshift cluster.
Checking cluster availability. Attempt 0/4.
Waiting 30 seconds before checking cluster availability.
Redshift cluster sparkify is available.
Opening Redshift cluster TCP port 5439 for external access.
Redshift cluster TCP port 5439 is already open.
Setting [IAM_ROLE][ARN]=arn:aws:iam::XXXXX:role/songplays_S3toRedshiftStaging in config file dwh.cfg.
Setting [CLUSTER][HOST]=sparkify.XXXX.us-west-2.redshift.amazonaws.com in config file dwh.cfg.
```

## Create Redshift Cluster Database Tables

Redshift cluster database tables are created by running the script `create_tables.py`. Tables are created according to how the schema is defined in sql_queries.py.  

```cmd
Dropping tables if they exist: [staging_events, staging_songs, songplay, users, songs, artists, time].
Creating tables: [staging_events, staging_songs, users, songs, artists, time, songplay].
```

## etl.py

The ETL process is performed by the script `etl.py`. Data is first loaded from AWS S3 buckets into staging tables in Redshift. From the staging tables data is inserted into the final tables used for analytical purposes.

Any errors loading data from S3 buckets into the staging tables are documented in a file. 

```cmd
Beginning copy from S3 bucket 's3://udacity-dend/log-data' into table staging_events.
Completed copy from S3 bucket 's3://udacity-dend/log-data' into table staging_events.
Beginning copy from S3 bucket 's3://udacity-dend/song-data/A' into table staging_songs.
Completed copy from S3 bucket 's3://udacity-dend/song-data/A' into table staging_songs.
Saving table staging_songs load errors into file load_errors_staging_songs.json.
Inserting data into table songplay.
Inserting data into table users.
Inserting data into table songs.
Inserting data into table artists.
Inserting data into table time.
```

## Delete Redshift Cluster (optional)

Running `infrastructure.py delete` automatically deletes the previously created Redshift cluster and associated IAM role.

```cmd
Deleting Redshift cluster sparkify.
Checking cluster deletion. Attempt 0/4.
Waiting 30 seconds before checking cluster deletion.
Redshift cluster sparkify has been deleted/does not exist.
Detaching S3 Policy from IAM Role songplays_S3toRedshiftStaging.
Deleting IAM Role songplays_S3toRedshiftStaging.
```
