import configparser
import time

import boto3
from botocore.exceptions import ClientError

config_file = 'dwh.cfg'

#  Load Configuration Parameters
config = configparser.ConfigParser()
config.optionxform = str
config.read(config_file)

# Create Clients
iam = boto3.client(
    'iam',
    aws_access_key_id=config['AWS']['KEY'],
    aws_secret_access_key=config['AWS']['SECRET'],
    region_name=config['AWS']['REGION']
)
redshift = boto3.client(
    'redshift',
    aws_access_key_id=config['AWS']['KEY'],
    aws_secret_access_key=config['AWS']['SECRET'],
    region_name=config['AWS']['REGION']
)

# Delete Redshift Cluster
print(f"Deleting Redshift cluster {config['CLUSTER']['CLUSTER_IDENTIFIER']}.")
try:
    redshift.delete_cluster(
        ClusterIdentifier=config['CLUSTER']['CLUSTER_IDENTIFIER'],
        SkipFinalClusterSnapshot=True
    )
except ClientError as err:
    if err.response['Error']['Code'] == 'ClusterNotFound':
        print(f"Redshift cluster {config['CLUSTER']['CLUSTER_IDENTIFIER']} does not exist.")
    else:
        raise
# wait for cluster to be deleted
attempt_max = 5
attempt_sec = 30
for attempt in range(0,attempt_max):
    try:
        print(f"Checking cluster deletion. Attempt {attempt}/{attempt_max-1}.")
        cluster = redshift.describe_clusters(
            ClusterIdentifier=config['CLUSTER']['CLUSTER_IDENTIFIER']
        )
        cluster = cluster['Clusters'][0]
        if attempt==attempt_max:
            raise RecursionError("Cluster deletion check max attempts exceeded. Suggest re-running teardown script.")
        else:
            print(f"Waiting {attempt_sec} seconds before rechecking cluster deletion.")
            time.sleep(attempt_sec)
    except ClientError as err:
        if err.response['Error']['Code'] == 'ClusterNotFound':
            print(f"Redshift cluster {config['CLUSTER']['CLUSTER_IDENTIFIER']} has been deleted/does not exist.")
            break
        else:
            raise

# Delete IAM Role for Redshift S3 Access
# detact policy
print(f"Detaching S3 Policy from IAM Role {config['IAM_ROLE']['ROLE_NAME']}.")
try:
    iam.detach_role_policy(
        RoleName=config['IAM_ROLE']['ROLE_NAME'],
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )
    print(f"Detached S3 Policy from IAM Role {config['IAM_ROLE']['ROLE_NAME']}.")
except ClientError as err:
    if err.response['Error']['Code'] == 'NoSuchEntity':
        print(f"S3 Policy for IAM Role {config['IAM_ROLE']['ROLE_NAME']} does not exist.")
    else:
        raise
# delete role
print(f"Deleting IAM Role {config['IAM_ROLE']['ROLE_NAME']}.")
try:
    iam.delete_role(RoleName=config['IAM_ROLE']['ROLE_NAME'])
    print(f"Redshift IAM Role {config['IAM_ROLE']['ROLE_NAME']} has been deleted.")
except ClientError as err:
    if err.response['Error']['Code'] == 'NoSuchEntity':
        print(f"Redshift IAM Role {config['IAM_ROLE']['ROLE_NAME']} does not exist.")
    else:
        raise

