''' Setup or teardown Redshift cluster and associated IAM role for user_song_plays ETL process.
'''

import configparser
import json
import time
import argparse

import boto3
from botocore.exceptions import ClientError

#  Load Configuration Parameters
config = configparser.ConfigParser()
config.optionxform = str
config.__path__ = 'dwh.cfg'
config.read(config.__path__)

# Create Clients
clients = {
    'iam': boto3.client(
        'iam',
        aws_access_key_id=config['AWS']['KEY'],
        aws_secret_access_key=config['AWS']['SECRET'],
        region_name=config['AWS']['REGION']
    ),
    'redshift': boto3.client(
        'redshift',
        aws_access_key_id=config['AWS']['KEY'],
        aws_secret_access_key=config['AWS']['SECRET'],
        region_name=config['AWS']['REGION']
    )
}

def setup(config, clients):

    # Create IAM Role for Redshift S3 Access
    print(f"Creating Redshift IAM Role {config['IAM_ROLE']['ROLE_NAME']}.")
    try:
        clients['iam'].create_role(
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
            print(f"Redshift IAM Role {config['IAM_ROLE']['ROLE_NAME']} already exists.")
        else:
            raise
    # attach policy
    print(f"Attaching Redshift IAM Role {config['IAM_ROLE']['ROLE_NAME']} policy for S3 read only access.")
    clients['iam'].attach_role_policy(
        RoleName=config['IAM_ROLE']['ROLE_NAME'],
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )['ResponseMetadata']['HTTPStatusCode']
    # set ARN in configuration file
    redshiftS3_arn = clients['iam'].get_role(RoleName=config['IAM_ROLE']['ROLE_NAME'])['Role']['Arn']
    print(f"Setting [IAM_ROLE][ARN]={redshiftS3_arn} in config file {config.__path__}.")
    config.set('IAM_ROLE','ARN',redshiftS3_arn)
    with open(config.__path__, 'w') as fh:
        config.write(fh)

    # Create Redshift Cluster
    print(f"Creating Redshift cluster {config['CLUSTER']['CLUSTER_IDENTIFIER']}.")
    print(f"Creating database {config['CLUSTER']['DB_NAME']} in Redshift cluster.")
    try:
        clients['redshift'].create_cluster(        
            # hardware
            ClusterType=config['CLUSTER']['CLUSTER_TYPE'],
            NodeType=config['CLUSTER']['NODE_TYPE'],
            NumberOfNodes=int(config['CLUSTER']['NUM_NODES']),
            # identifiers
            DBName=config['CLUSTER']['DB_NAME'],
            ClusterIdentifier=config['CLUSTER']['CLUSTER_IDENTIFIER'],
            # credentials
            MasterUsername=config['CLUSTER']['DB_USER'],
            MasterUserPassword=config['CLUSTER']['DB_PASSWORD'],
            # roles
            IamRoles=[redshiftS3_arn]
        )
    except ClientError as err:
        if err.response['Error']['Code'] == 'ClusterAlreadyExists':
            print(f"Redshift cluster {config['CLUSTER']['CLUSTER_IDENTIFIER']} already exists.")
        else:
            raise
    # wait for cluster to become avaliable
    attempt_max = 5
    attempt_sec = 30
    for attempt in range(0,attempt_max):
        print(f"Checking cluster availability. Attempt {attempt}/{attempt_max-1}.")
        cluster = clients['redshift'].describe_clusters(
            ClusterIdentifier=config['CLUSTER']['CLUSTER_IDENTIFIER']
        )
        cluster = cluster['Clusters'][0]
        if cluster['ClusterStatus']=='available':
            print(f"Redshift cluster {config['CLUSTER']['CLUSTER_IDENTIFIER']} is available.")
            break
        elif attempt==attempt_max:
            raise RecursionError("Cluster availability check max attempts exceeded. Suggest re-running setup script.")
        else:
            print(f"Waiting {attempt_sec} seconds before rechecking cluster availability.")
            time.sleep(attempt_sec)

def teardown(config, clients):

    # Delete Redshift Cluster
    print(f"Deleting Redshift cluster {config['CLUSTER']['CLUSTER_IDENTIFIER']}.")
    try:
        clients['redshift'].delete_cluster(
            ClusterIdentifier=config['CLUSTER']['CLUSTER_IDENTIFIER'],
            SkipFinalClusterSnapshot=True
        )
    except ClientError as err:
        if err.response['Error']['Code'] == 'ClusterNotFound':
            print(f"Redshift cluster {config['CLUSTER']['CLUSTER_IDENTIFIER']} does not exist.")
        else:
            raise
    # wait for cluster to be deleted
    for attempt in range(0,config['STAUS_CHECK_ATTEMPTS']):
        try:
            print(f"Checking cluster deletion. Attempt {attempt}/{config['STAUS_CHECK_ATTEMPTS']-1}.")
            cluster = clients['redshift'].describe_clusters(
                ClusterIdentifier=config['CLUSTER']['CLUSTER_IDENTIFIER']
            )
            cluster = cluster['Clusters'][0]
            if attempt==config['STAUS_CHECK_ATTEMPTS']:
                raise RecursionError("Cluster deletion check max attempts exceeded. Suggest re-running teardown script.")
            else:
                print(f"Waiting {config['STATUS_CHECK_DELAY_SEC']} seconds before rechecking cluster deletion.")
                time.sleep(config['STATUS_CHECK_DELAY_SEC'])
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
        clients['iam'].detach_role_policy(
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
        clients['iam'].delete_role(RoleName=config['IAM_ROLE']['ROLE_NAME'])
        print(f"Redshift IAM Role {config['IAM_ROLE']['ROLE_NAME']} has been deleted.")
    except ClientError as err:
        if err.response['Error']['Code'] == 'NoSuchEntity':
            print(f"Redshift IAM Role {config['IAM_ROLE']['ROLE_NAME']} does not exist.")
        else:
            raise

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('plan', type=str, choices=['setup','teardown'])

    args = parser.parse_args()

    if args['plan'] == 'setup':
        setup(config, clients)
    elif args['plan'] == 'teardown':
        teardown(config, clients)