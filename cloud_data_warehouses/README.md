# Sparkify User Song Plays

An Amazon Web Services (AWS) Extract-Transform-Load (ETL) process that allows queries to answer analytical questions about user song plays.

## Quick Start

Below is a brief overview of the steps to perform. More detail is included in additional sections.

1. Activate a Python environment and install required packages defined in `requirements.txt`.
2. Set configuration parameters in `dwh.cfg`.
3. (Optional for Infrastructure as Code) Define additional [INFRASTRUCTURE] section in `dwh.cfg`. INFRASTRUCTURE/KEY and INFRASTRUCTURE/SECRET are manually generated in the AWS console.
4. (Optional for Infrastructure as Code) Install requirements using `requirements_iac.txt`.
5. (Optional for Infrastructure as Code) Execute `infastructure.py create` to create Redshift infrastructure.
6. Execute `create_tables.py`.
7. Execute `etl.py`.
8. Execute `test_etl.py` to ensure primary keys are unique and also display size of tables.
9. (Optional for Dashboard) Install requirements using `requirements_dashboard.txt`.
10. (Optional for Dashboard) Execute `dashboard.py` to display the analytical dashboard.
11. (Optional for Infrastructure as Code) Execute `infastructure.py delete` to delete Redshift infrastructure.

## dwh.cfg

The following fields are required.

```cfg
[CLUSTER]
HOST = XXX
DB_NAME = XXX
DB_USER = XXX
DB_PASSWORD = XXX
DB_PORT = 5439

[IAM_ROLE]
ARN = XXX

[S3]
LOG_DATA = 's3://udacity-dend/log-data'
LOG_JSONPATH = 's3://udacity-dend/log_json_path.json'
SONG_DATA = 's3://udacity-dend/song-data'
```

The additional section is required for running infrastucture.py to create/delete Redshift infrastructure.

```cfg
[INFRASTRUCTURE]
REGION = XXX
KEY = XXX
SECRET = XXX
STATUS_CHECK_ATTEMPTS = 5
STATUS_CHECK_DELAY_SEC = 30
ROLE_NAME = songplays_S3toRedshiftStaging
CLUSTER_IDENTIFIER = sparkify
CLUSTER_TYPE = multi-node
NODE_TYPE = dc2.large
NUM_NODES = 2
```

NFRASTRUCTURE/KEY and INFRASTRUCTURE/SECRET are manually generated in the AWS console using the steps below.

1. Navigate to IAM / Access Management / Users / Add users
2. User name = user_song_plays_setup
3. Select AWS credential type = Access key - Programatic access
4. Next: Permissions
5. Attach existing policies directly
6. Select AdministratorAccess
7. Next: Tags
8. Next: Review
9. Create user
10. Copy the provided values to the NFRASTRUCTURE section of dwh.cfg


<!-- TODO: document starting here -->

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

Dropping tables if they exist: ['staging_events', 'staging_songs', 'songplay', 'users', 'songs', 'artists', 'time'].
Creating tables: ['staging_events', 'staging_songs', 'users', 'songs', 'artists', 'time', 'songplay'].

## ETL

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

Any errors loading data from S3 buckets into the staging tables are documented in a file named according to the name of the staging table.

```cmd
Beginning copy from S3 bucket 's3://udacity-dend/log-data' into table staging_events.
Completed copy from S3 bucket 's3://udacity-dend/log-data' into table staging_events.
No errors occured loading into table staging_events.
Beginning copy from S3 bucket 's3://udacity-dend/song-data/A/Y/F' into table staging_songs.
Completed copy from S3 bucket 's3://udacity-dend/song-data/A/Y/F' into table staging_songs.
Saving table staging_songs load errors into file stl_load_errors_staging_songs.json.
Inserting data into table songplay.
Inserting data into table users.
Inserting data into table songs.
Inserting data into table artists.
Inserting data into table time.
```

An example of the contents of stl_load_errors_staging_songs.json is shown below. Each top level key represents a column in the stage table. Values are a list of errors that occured for each source S3 file.

```json
{
    "artist_location": [
        {
            "err_reason": "String length exceeds DDL length",
            "filename": "s3://udacity-dend/song-data/A/Y/F/TRAYFUW128F428F618.json"
        },
        {
            "err_reason": "String length exceeds DDL length",
            "filename": "s3://udacity-dend/song-data/A/Y/F/TRAYFUW128F428F618.json"
        }
    ]
}
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

## Dashboard

Aggregations performed server side to fully utilize performance of Redshift's columnar data store.

## ETL Improvements

|user_id |session_id  |level  |start_time  |play_count |
|--------|------------|-------|------------|-----------|
|15      |172         |paid   |2018-11-02  |74         |
|15      |199         |paid   |2018-11-03  |27         |
|15      |221         |paid   |2018-11-07  |101        |
|15      |362         |paid   |2018-11-09  |9          |
|15      |417         |paid   |2018-11-13  |2          |
|15      |557         |paid   |2018-11-14  |18         |
|15      |582         |paid   |2018-11-15  |5          |
|15      |612         |paid   |2018-11-19  |10         |
|15      |716         |paid   |2018-11-20  |59         |
|15      |818         |paid   |2018-11-21  |70         |
|15      |764         |free   |2018-11-21  |66         |
|15      |834         |paid   |2018-11-26  |22         |


|user_id |index | current |previous |next |sessions |play_count |level_days |start_time |
|--------|------|---------|---------|-----|---------|-----------|-----------|-----------|
|15      |1     |paid     |free     |null |2        |92         |4          |2018-11-21 |
|15      |2     |free     |paid     |paid |1        |66         |0          |2018-11-21 |
|15      |3     |paid     |null     |free |9        |305        |17         |2018-11-02 |
