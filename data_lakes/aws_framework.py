import configparser
import argparse
from typing import Literal
import boto3
from botocore.exceptions import ClientError


def output_s3_bucket(config, action:Literal['create','delete']):
    ''' 
    TODO: document
    
    Parameters
    ----------
    TODO: document

    Returns
    -------
    TODO: document

    '''
    
    client =  boto3.client(
        's3',
        aws_access_key_id=config['DEFAULT']['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['DEFAULT']['AWS_SECRET_ACCESS_KEY'],
        region_name=config['DEFAULT']['REGION_NAME']
    )

    try:
        print(f"output s3 bucket {action} begin: {config['S3']['OUTPUT_BUCKET_NAME']}")
        if action=='create':
            client.create_bucket(
                ACL='private',
                Bucket=config['S3']['OUTPUT_BUCKET_NAME'],
                CreateBucketConfiguration={
                    'LocationConstraint': config['DEFAULT']['REGION_NAME'],
                }
            )
        elif action=='delete':
            client.delete_bucket(Bucket=config['S3']['OUTPUT_BUCKET_NAME'])
        else:
            raise RuntimeError("action argument must be one of ['create', 'delete']")
        print(f"output s3 bucket {action} successful: {config['S3']['OUTPUT_BUCKET_NAME']}")
    except ClientError as error:
        if error.response['Error']['Code'] in ['BucketAlreadyOwnedByYou', 'NoSuchBucket']:
            print(f"{error.response['Error']['Code']}: {config['S3']['OUTPUT_BUCKET_NAME']}")
        else:
            raise error


def emr_cluster(config, action:Literal['create','delete']):
    ''' 
    TODO: document
    
    Parameters
    ----------
    TODO: document

    Returns
    -------
    TODO: document

    '''
    client =  boto3.client(
        'emr',
        aws_access_key_id=config['DEFAULT']['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['DEFAULT']['AWS_SECRET_ACCESS_KEY'],
        region_name=config['DEFAULT']['REGION_NAME']
    )

    # https://stackoverflow.com/questions/26314316/how-to-launch-and-configure-an-emr-cluster-using-boto
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.run_job_flow

    try:
        print(f"emr studio {action} begin: {config['EMR']['CLUSTER_NAME']}")
        if action=='create':
            client.run_job_flow(
                Name=config['EMR']['CLUSTER_NAME'],
                LogUri=f"s3://{config['S3']['OUTPUT_BUCKET_NAME']}",
                ReleaseLabel='emr-5.34.0',
                Instances={
                    'MasterInstanceType': 'm3.xlarge',
                    'SlaveInstanceType': 'm3.xlarge',
                    'InstanceCount': 3,
                    'KeepJobFlowAliveWhenNoSteps': True,
                    'TerminationProtected': False,
                    # 'Ec2SubnetId': 'my-subnet-id',
                    # 'Ec2KeyName': 'my-key',
                },
                JobFlowRole='EMR_EC2_DefaultRole',
                ServiceRole='EMR_DefaultRole',
                Applications=[
                    {
                        'Name': 'Spark'
                    }
    ],
            )
        elif action=='delete':
            pass
        else:
            raise RuntimeError("action argument must be one of ['create', 'delete']")
        print(f"emr studio {action} successful: {config['EMR']['CLUSTER_NAME']}")
    except ClientError as error:
        raise error
        # if error.response['Error']['Code'] in ['BucketAlreadyOwnedByYou', 'NoSuchBucket']:
        #     print(f"{error.response['Error']['Code']}: {config['EMR']['CLUSTER_NAME']}")
        # else:
        #     raise error

def billing_alert(config):
    pass

if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read('dl.cfg')

    parser = argparse.ArgumentParser()
    parser.add_argument('action', type=str, choices=['create','delete'])
    args = parser.parse_args()

    output_s3_bucket(config, args.action)
    # emr_cluster(config, action=args.action)
    # billing_alert(config, action=args.action)