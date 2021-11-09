''' Set up Redshift Cluster and associated IAM role for user_song_plays ETL process.

First a few manual steps must be completed to grant programatic access for this script.

Manual Steps
------------
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
10a. KEY = Access key ID
10b. SECRET = Secret access key
'''

import configparser
import json

import boto3
from botocore.exceptions import ClientError

config_file = 'dwh.cfg'

#  Load Configuration Parameters
config = configparser.ConfigParser()
config.read(config_file)

# Create Clients
iam = boto3.client(
    'iam',aws_access_key_id=config['AWS']['KEY'],
    aws_secret_access_key=config['AWS']['SECRET'],
    region_name=config['AWS']['REGION']
)

# Create IAM Role for Redshift S3 Access
print(f"Creating Redshift IAM Role {config['IAM_ROLE']['ROLE_NAME']}.")
try:
    role_dwh = iam.create_role(
        Path='/',
        RoleName=config['IAM_ROLE']['ROLE_NAME'],
        Description = "Allow Redshift AWS service access.",
        AssumeRolePolicyDocument=json.dumps({
            'Statement': [{
                'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}
            }],
            'Version': '2012-10-17'})
    )
except ClientError as err:
    if err.response['Error']['Code'] == 'EntityAlreadyExists':
        print(f"Skip creating Redshift IAM Role {config['IAM_ROLE']['ROLE_NAME']}. Role already exists.")
    else:
        raise
# attach policy
print(f"Attaching Redshift IAM Role {config['IAM_ROLE']['ROLE_NAME']} policy for S3 read only access.")
iam.attach_role_policy(
    RoleName=config['IAM_ROLE']['ROLE_NAME'],
    PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
)['ResponseMetadata']['HTTPStatusCode']
# set ARN in configuration file
role_arn = iam.get_role(RoleName=config['IAM_ROLE']['ROLE_NAME'])['Role']['Arn']
print(f"Setting [IAM_ROLE][ARN]={role_arn} in {config_file}.")
config.set('IAM_ROLE','ARN',role_arn)
with open(config_file, 'w') as fh:
    config.write(fh)