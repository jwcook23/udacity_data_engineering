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
10. Copy the provided values to the IAM_ROLE section of dwh.cfg
10a. ARN = User
10b. KEY = Access key ID
10c. SECRET = Secret access key
'''

import configparser

import boto3

#  Load Configuration Parameters
config = configparser.ConfigParser()
config.read('dwh.cfg')
config['IAM_ROLE']['ARN']
config['IAM_ROLE']['KEY']
config['IAM_ROLE']['SECRET']

