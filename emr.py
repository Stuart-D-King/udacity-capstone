import configparser
import boto3
import time
import os
from botocore.exceptions import ClientError


def create_emr_client(key, secret):
    '''
    Using the Amazon Web Services (AWS) SDK for Python - boto3 - and the passed in AWS access key and secret key, create and return an EMR client

    INPUT
    key: AWS access key string
    secret: AWS secret key string

    OUTPUT
    emr: AWS Elastic Map Reduce (EMR) boto3 resource instance
    '''
    try:
        emr = boto3.client('emr',
            region_name="us-west-2",
            aws_access_key_id=key,
            aws_secret_access_key=secret)
        return emr

    except Exception as e:
        print(e)


def create_emr_cluster(emr, emr_name, log_uri, node_type, key_pair, subnet, bucket):
    '''
    Create a new EMR cluster in Virtual Private Cloud (VPC)

    INPUT
    emr: AWS EMR boto3 client instance
    emr_name: name for EMR cluster
    log_ur: destination where log files should be stored
    node_type: node type to be provisioned for the cluster
    key_pair: name of the Amazon EC2 key pair to use when connecting with SSH into the master node as a user named "hadoop"
    subnet: identifier of the VPC subnet where you want the cluster to launch
    bucket: s3 bucket name

    OUTPUT
    response: dictionary of newly created cluster parameters
    '''
    try:
        response = emr.run_job_flow(
            Name=emr_name,
            LogUri=log_uri,
            ReleaseLabel='emr-5.29.0',
            Applications=[{'Name': 'Spark'}, {'Name': 'Ganglia'}, {'Name': 'Zeppelin'}],
            Instances={
                'InstanceGroups': [
                    {'Name': 'master_node',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': node_type,
                        'InstanceCount': 1},
                    {'Name': 'slave_node',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': node_type,
                        'InstanceCount': 3}],
                'Ec2KeyName': key_pair,
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': subnet},
            BootstrapActions=[
                {'Name': 'install_python_modules',
                    'ScriptBootstrapAction': {
                        'Path': 's3://' + bucket + '/install_python_modules.sh'}
                }],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole'
        )

        return response

    except Exception as e:
        print(e)


def add_cluster_steps(emr, cluster_id, bucket):
    '''
    Add and run action steps to EMR cluster

    INPUT
    emr: AWS EMR boto3 client instance
    cluster_id: Cluster ID for EMR cluster to add steps to
    bucket: S3 bucket name

    OUTPUT
    none
    '''
    try:
        # copy etl.py to EMR local
        fname = fname = 's3://' + bucket + '/etl.py'
        copy_args = ['aws', 's3', 'cp', fname, '/home/hadoop/etl.py']
        copy_script_step ={
            'Name': 'copy_pipeline_to_emr',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': copy_args
                }
            }

        # submit etl.py to spark-submit
        spark_args = ['spark-submit', '--packages', 'saurfang:spark-sas7bdat:2.1.0-s_2.11', '--master', 'yarn', '/home/hadoop/etl.py']
        spark_step = {
            'Name': 'run-capstone-pipeline',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': spark_args
                }
            }

        actions = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[copy_script_step, spark_step])

        print('Added steps: {}'.format(actions))

    except Exception as e:
        print(e)


def terminate_cluster(emr, cluster_id):
    '''
    Terminate EMR cluster

    INPUT
    emr: AWS EMR boto3 client instance
    cluster_id: Cluster ID for EMR cluster to terminate

    OUTPUT
    none
    '''
    try:
        response = emr.terminate_job_flows(JobFlowIds=[cluster_id])
        print('Terminating EMR cluster: {}'.format(cluster_id))

    except Exception as e:
        print(e)


def main():
    '''
    Create a new EMR cluster and run actions on the cluster. Terminate cluster after steps complete.
    '''
    config = configparser.ConfigParser()
    config.read('emr.cfg')

    key = config.get('AWS', 'AWS_ACCESS_KEY_ID')
    secret = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

    emr = create_emr_client(key, secret)

    response = create_emr_cluster(emr,
        config.get('CLUSTER', 'NAME'),
        config.get('CLUSTER', 'LOG_URI'),
        config.get('CLUSTER', 'NODE_TYPE'),
        config.get('CLUSTER', 'KEY_PAIR'),
        config.get('CLUSTER', 'SUBNET'),
        config.get('AWS', 'S3_BUCKET')
    )

    cluster_id = response['JobFlowId']

    timeout = time.time() + 60*15 # 15 minute timeout
    status = 'STARTING'
    print('Waiting for cluster to become available...')
    while time.time() < timeout:
        if status.upper() == 'WAITING': break
        time.sleep(5)
        status = emr.describe_cluster(ClusterId=cluster_id)['Cluster']['Status']['State']

    if status.upper() == 'WAITING':
        print('Cluster is available!')
        print('Starting Spark job...')
        add_cluster_steps(emr, cluster_id, config.get('AWS', 'S3_BUCKET'))
    else:
        print('Cluster never became available')


if __name__ == "__main__":
    main()
