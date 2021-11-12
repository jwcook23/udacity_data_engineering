# Sparkify User Song Plays

An Amazon Web Services (AWS) Extract-Transform-Load (ETL) pipeline that allows analytical queries to answer questions about user song plays.

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

    TODO: parallel loading is used to increase the speed of loading data

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
Detached S3 Policy from IAM Role songplays_S3toRedshiftStaging.
Deleting IAM Role songplays_S3toRedshiftStaging.
Redshift IAM Role songplays_S3toRedshiftStaging has been deleted.
```
