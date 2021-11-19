import os
import glob
import boto3
import configparser


def get_filepaths(filepath):
    '''
    Get paths to all SAS files in passed-in directory filepath

    INPUT
    filepath: filepath string of directory

    OUTPUT
    List of all files within passed-in directory path
    '''
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.sas7bdat'))
        for f in files :
            all_files.append(os.path.abspath(f))

    return all_files


def create_s3_client(key, secret):
    '''
    Using the Amazon Web Services (AWS) SDK for Python - boto3 - and the passed in AWS access key and secret key, create and return an S3 client

    INPUT
    key: AWS access key string
    secret: AWS secret key string

    OUTPUT
    s3: AWS Simple Storage Service (S3) boto3 client instance
    '''
    try:
        s3 = boto3.client('s3',
            region_name='us-west-2',
            aws_access_key_id=key,
            aws_secret_access_key=secret)

        return s3

    except Exception as e:
        print(e)


def main():
    config = configparser.ConfigParser()
    config.read('emr.cfg')
    bucket = config.get('AWS', 'S3_BUCKET')
    key = config.get('AWS', 'AWS_ACCESS_KEY_ID')
    secret = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

    s3 = create_s3_client(key, secret)

    # immigraton data
    sas_dir = '../../data/18-83510-I94-Data-2016'
    sas_filepaths = get_filepaths(sas_dir)
    for f in sas_filepaths:
        s3.upload_file('../..{}'.format(f), bucket, 'data/sas_data/{}'.format(f.split('/')[-1]))

    # airport data
    airport_file = 'data/airport-codes_csv.csv'
    s3.upload_file(airport_file, bucket, airport_file)

    # city demographics data
    demo_file = 'data/us-cities-demographics.csv'
    s3.upload_file(demo_file, bucket, demo_file)

    # temperature data
    temp_file = '../../data2/GlobalLandTemperaturesByCity.csv'
    s3.upload_file(temp_file, bucket, 'data/{}'.format(temp_file.split('/')[-1]))

    # etl.py
    s3.upload_file('etl.py', bucket, 'etl.py')

    # intstall_python_modules.sh
    s3.upload_file('install_python_modules.sh', bucket, 'install_python_modules.sh')


if __name__ == "__main__":
    main()
