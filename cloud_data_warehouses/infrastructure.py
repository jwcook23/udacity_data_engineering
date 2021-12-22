''' Create or delete a Redshift cluster and associated IAM role for user song plays ETL process.

Creating can be ran numerous times without causing multiple resources to be created or errors 
to be raised. Deleting can likewise be ran numerous times. If creating infrastructure, values 
are also written to the configuration file for the [IAM_ROLE][ARN] and [CLUSTER][HOST] 
so they do not have to be manually copied.

Parameters
----------
plan (str) : infrastructure plan to perform, options are 'create' or 'delete'

Returns
-------
None

See Also
--------
dwh.cfg

Create Example
--------------
infrastructure.py create

Delete Example
--------------
infrastructure.py delete

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
        aws_access_key_id=config['INFRASTRUCTURE']['KEY'],
        aws_secret_access_key=config['INFRASTRUCTURE']['SECRET'],
        region_name=config['INFRASTRUCTURE']['REGION']
    ),
    'redshift': boto3.client(
        'redshift',
        aws_access_key_id=config['INFRASTRUCTURE']['KEY'],
        aws_secret_access_key=config['INFRASTRUCTURE']['SECRET'],
        region_name=config['INFRASTRUCTURE']['REGION']
    ),
    'ec2': boto3.resource(
        'ec2',
        aws_access_key_id=config['INFRASTRUCTURE']['KEY'],
        aws_secret_access_key=config['INFRASTRUCTURE']['SECRET'],
        region_name=config['INFRASTRUCTURE']['REGION']
    )
}

def create(config, clients):

    STATUS_CHECK_ATTEMPTS = int(config['INFRASTRUCTURE']['STATUS_CHECK_ATTEMPTS'])
    STATUS_CHECK_DELAY_SEC = int(config['INFRASTRUCTURE']['STATUS_CHECK_DELAY_SEC'])

    # Create IAM Role for Redshift S3 Access
    print(f"Creating Redshift IAM Role {config['INFRASTRUCTURE']['ROLE_NAME']}.")
    try:
        clients['iam'].create_role(
            Path='/',
            RoleName=config['INFRASTRUCTURE']['ROLE_NAME'],
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
            print(f"Redshift IAM Role {config['INFRASTRUCTURE']['ROLE_NAME']} already exists.")
        else:
            raise
    # attach policy
    print(f"Attaching Redshift IAM Role {config['INFRASTRUCTURE']['ROLE_NAME']} policy for S3 read only access.")
    clients['iam'].attach_role_policy(
        RoleName=config['INFRASTRUCTURE']['ROLE_NAME'],
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )['ResponseMetadata']['HTTPStatusCode']
    # get Redshift S3 access resource name set ARN in configuration file
    redshiftS3_arn = clients['iam'].get_role(RoleName=config['INFRASTRUCTURE']['ROLE_NAME'])['Role']['Arn']

    # Create Redshift Cluster
    print(f"Creating Redshift cluster {config['INFRASTRUCTURE']['CLUSTER_IDENTIFIER']}.")
    print(f"Creating database {config['CLUSTER']['DB_NAME']} in Redshift cluster.")
    try:
        clients['redshift'].create_cluster(        
            # hardware
            ClusterType=config['INFRASTRUCTURE']['CLUSTER_TYPE'],
            NodeType=config['INFRASTRUCTURE']['NODE_TYPE'],
            NumberOfNodes=int(config['INFRASTRUCTURE']['NUM_NODES']),
            # identifiers
            DBName=config['CLUSTER']['DB_NAME'],
            ClusterIdentifier=config['INFRASTRUCTURE']['CLUSTER_IDENTIFIER'],
            # credentials and access
            MasterUsername=config['CLUSTER']['DB_USER'],
            MasterUserPassword=config['CLUSTER']['DB_PASSWORD'],
            Port=int(config['CLUSTER']['DB_PORT']),
            # roles
            IamRoles=[redshiftS3_arn]
        )
    except ClientError as err:
        if err.response['Error']['Code'] == 'ClusterAlreadyExists':
            print(f"Redshift cluster {config['INFRASTRUCTURE']['CLUSTER_IDENTIFIER']} already exists.")
        else:
            raise
    # wait for cluster to become avaliable
    cluster_host = None
    for attempt in range(0,STATUS_CHECK_ATTEMPTS):
        print(f"Checking cluster availability. Attempt {attempt}/{STATUS_CHECK_ATTEMPTS-1}.")
        print(f"Waiting {STATUS_CHECK_DELAY_SEC} seconds before checking cluster availability.")
        time.sleep(STATUS_CHECK_DELAY_SEC)
        cluster = clients['redshift'].describe_clusters(
            ClusterIdentifier=config['INFRASTRUCTURE']['CLUSTER_IDENTIFIER']
        )
        cluster = cluster['Clusters'][0]
        if cluster['ClusterStatus']=='available':
            print(f"Redshift cluster {config['INFRASTRUCTURE']['CLUSTER_IDENTIFIER']} is available.")
            cluster_host = cluster['Endpoint']['Address']
            break
        elif attempt==STATUS_CHECK_ATTEMPTS:
            raise RecursionError("Cluster availability check max attempts exceeded. Suggest re-running setup script.")

    # open incoming TCP port to access the cluster externally
    print(f"Opening Redshift cluster TCP port {config['CLUSTER']['DB_PORT']} for external access.")
    try:
        vpc = clients['ec2'].Vpc(id=cluster['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(config['CLUSTER']['DB_PORT']),
            ToPort=int(config['CLUSTER']['DB_PORT'])
        )
    except ClientError as err:
        if err.response['Error']['Code'] == 'InvalidPermission.Duplicate':
            print(f"Redshift cluster TCP port {config['CLUSTER']['DB_PORT']} is already open.")
        else:
            raise

    # save values in config file
    print(f"Setting [IAM_ROLE][ARN]={redshiftS3_arn} in config file {config.__path__}.")
    print(f"Setting [CLUSTER][HOST]={cluster_host} in config file {config.__path__}.")
    config.set('IAM_ROLE','ARN',redshiftS3_arn)
    config.set('CLUSTER','HOST',cluster_host)
    with open(config.__path__, 'w') as fh:
        config.write(fh)

def delete(config, clients):

    STATUS_CHECK_ATTEMPTS = int(config['INFRASTRUCTURE']['STATUS_CHECK_ATTEMPTS'])
    STATUS_CHECK_DELAY_SEC = int(config['INFRASTRUCTURE']['STATUS_CHECK_DELAY_SEC'])

    # Delete Redshift Cluster
    print(f"Deleting Redshift cluster {config['INFRASTRUCTURE']['CLUSTER_IDENTIFIER']}.")
    try:
        clients['redshift'].delete_cluster(
            ClusterIdentifier=config['INFRASTRUCTURE']['CLUSTER_IDENTIFIER'],
            SkipFinalClusterSnapshot=True
        )
    except ClientError as err:
        if err.response['Error']['Code'] == 'ClusterNotFound':
            print(f"Redshift cluster {config['INFRASTRUCTURE']['CLUSTER_IDENTIFIER']} does not exist.")
        else:
            raise
    # wait for cluster to be deleted
    for attempt in range(0, STATUS_CHECK_ATTEMPTS ):
        try:
            print(f"Checking cluster deletion. Attempt {attempt}/{STATUS_CHECK_ATTEMPTS-1}.")
            print(f"Waiting {STATUS_CHECK_DELAY_SEC} seconds before checking cluster deletion.")
            time.sleep(STATUS_CHECK_DELAY_SEC)
            cluster = clients['redshift'].describe_clusters(
                ClusterIdentifier=config['INFRASTRUCTURE']['CLUSTER_IDENTIFIER']
            )
            cluster = cluster['Clusters'][0]
            if attempt==STATUS_CHECK_ATTEMPTS:
                raise RecursionError("Cluster deletion check max attempts exceeded. Suggest re-running teardown script.")
        except ClientError as err:
            if err.response['Error']['Code'] == 'ClusterNotFound':
                print(f"Redshift cluster {config['INFRASTRUCTURE']['CLUSTER_IDENTIFIER']} has been deleted/does not exist.")
                break
            else:
                raise

    # Delete IAM Role for Redshift S3 Access
    # detact policy
    print(f"Detaching S3 Policy from IAM Role {config['INFRASTRUCTURE']['ROLE_NAME']}.")
    try:
        clients['iam'].detach_role_policy(
            RoleName=config['INFRASTRUCTURE']['ROLE_NAME'],
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )
    except ClientError as err:
        if err.response['Error']['Code'] == 'NoSuchEntity':
            print(f"S3 Policy for IAM Role {config['INFRASTRUCTURE']['ROLE_NAME']} does not exist.")
        else:
            raise
    # delete role
    print(f"Deleting IAM Role {config['INFRASTRUCTURE']['ROLE_NAME']}.")
    try:
        clients['iam'].delete_role(RoleName=config['INFRASTRUCTURE']['ROLE_NAME'])
    except ClientError as err:
        if err.response['Error']['Code'] == 'NoSuchEntity':
            print(f"Redshift IAM Role {config['INFRASTRUCTURE']['ROLE_NAME']} does not exist.")
        else:
            raise

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('plan', type=str, choices=['create','delete'])

    args = parser.parse_args()

    if args.plan == 'create':
        create(config, clients)
    elif args.plan == 'delete':
        delete(config, clients)