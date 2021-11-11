# Sparkify User Song Plays

An Amazon Web Services (AWS) Extract-Transform-Load (ETL) pipeline that allows analytical queries to answer questions about user song plays.

## Quick Start

1. Create Virtual Environment

``` cmd
python -m venv user_song_plays
.\user_song_plays\Scripts\activate
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

## Automated Setup and Teardown

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
10a. [AWS][KEY] = Access key ID
10b. [AWS][SECRET] = Secret access key