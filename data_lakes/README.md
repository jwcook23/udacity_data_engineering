# Sparkify Spark Data Lake

<!-- TODO: rewrite this overview-->
An ETL pipeline that extracts data from AWS S3, processes the data using Spark, and loads the data back into S3 as a set of dimensional tables.

## Source S3 Data

The source S3 data resides in AWS Region US West (Oregon) us-west-2 under the Access Resource Name `arn:aws:s3:::udacity-dend`.

[Log Data](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend/log_data/)

[Song Data](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend/song_data/)

## Quickstart

<!-- TODO: minimum steps to run -->

## Setup

1. Create a configuration file named `dl.cfg` with the following structure.

    ```cfg
    AWS_ACCESS_KEY_ID=***
    AWS_SECRET_ACCESS_KEY=***
    REGION=us-west-2
    ```

2. Using the AWS console, create an IAM user for programmatic access to AWS. Add the created credentials to `dl.cfg`.

    1. IAM > Users > Add users
    2. Enter User name = udacity_data_lake (anything to uniquely identify)
    3. Select AWS access type = "Access key - Programmatic access"
    4. Select Next: Permissions
    5. Select Create group
    6. Enter Group name = udacity_spark (anything to uniquely identify)
    7. Select the following policies
        - AmazonS3FullAccess
        - AmazonEMRFullAccessPolicy_v2
        - AWSBudgetsActionsWithAWSResourceControlAccess
    8. Select Create group
    9. Select Next: tag
    10. Select Next: Review
    11. Select Create user
    12. Copy key and secret to `dl.cfg`
        - AWS_ACCESS_KEY_ID = Access key ID
        - AWS_SECRET_ACCESS_KEY = Secret access key

3. In the AWS console, create an EMR IAM role.

    1. IAM > Roles > Create role
    2. Trusted entity type = AWS service
    3. Use case = EMR
    4. Select Next
    5. Select Next on Add permissions
    6. Enter Role name = emr_default (anything to uniquely identify)
    7. Select Create role

Create and activate virtual environment.

```cmd
python -m venv env
.\env\Scripts\activate
```

```cmd
pip install -r requirements.txt
```
