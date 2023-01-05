import os
import logging
import json
import boto3
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
import requests

bucket_name = "dhsoni-boto3"
region_name = "us-east-2"


# creating bucket
def create_bucket(bucket_name, region=None):
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True
create_bucket(bucket_name, region_name)

# listing bucket
def list_buckets( region=None):
    s3_client = boto3.client('s3')
    try:
        if region is not None:
            s3_client = boto3.client('s3', region_name=region)
        response = s3_client.list_buckets()
        print('Existing buckets:')
        for bucket in response['Buckets']:
            print(f'  {bucket["Name"]}')
    except ClientError as e:
        logging.error(e)
        return False
    return True

list_buckets(region_name)

# uploading a file to bucket
def upload_file(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_name)

    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True
upload_file("./sample_file_3.txt", bucket_name, "sample_file_3.txt")

# uploading a file object to bucket
def upload_file_object(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        with open(file_name, "rb") as f:
            s3_client.upload_fileobj(f, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True
upload_file_object("./sample_file_2.txt", bucket_name, "sample_file_2.txt")

# creating an empty bucket
create_bucket("dhsoni-empty-bucket", region_name)

# deleting empty bucket
def delete_empty_bucket(bucket):
    s3_client = boto3.client('s3')
    response = s3_client.delete_bucket(Bucket=bucket)
    print(response)
delete_empty_bucket("dhsoni-empty-bucket")

# deleting object from bucket
def delete_object(bucket,object_name):
    s3_client = boto3.client('s3')
    response = s3_client.delete_object(Bucket=bucket,Key=object_name)
delete_object(bucket_name, "sample_file_2.txt")

# deleting non empty bucket
def delete_non_empty_bucket(bucket):
    s3_client = s3 = boto3.resource('s3') 
    bucketClient = s3_client.Bucket(bucket)
    bucketClient.objects.all().delete()
    bucketClient.meta.client.delete_bucket(Bucket=bucket)
delete_non_empty_bucket(bucket_name)

# downloading file from bucket
def download_file(file_name, bucket, object_name):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.download_file(bucket, object_name, file_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True
download_file("./downloaded_files/sample_file.txt", bucket_name, "sample_file.txt")

# downloading file object from bucket
def download_file_object(file_name, bucket, object_name):
    s3_client = boto3.client('s3')
    try:
        with open(file_name, "wb") as f:
            s3_client.download_fileobj(bucket, object_name, f)
    except ClientError as e:
        logging.error(e)
        return False
    return True
download_file_object("./downloaded_files/sample_file_2.txt", bucket_name, "sample_file_2.txt")

# uploading file in multipart
def upload_file_multipart(file_name, bucket, object_name=None):
    GB = 1024 ** 3
    config = TransferConfig(multipart_threshold=5*GB)

    if object_name is None:
        object_name = os.path.basename(file_name)

    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name, Config=config)
    except ClientError as e:
        logging.error(e)
        return False
    return True
upload_file_multipart("./sample_file.txt", bucket_name, "sample_file.txt")

# downloading file concurrently
def download_file_concurrently(file_name, bucket, object_name):
    #default concurrency is 10
    config = TransferConfig(max_concurrency=20)
    s3_client = boto3.client('s3')
    try:
        response = s3_client.download_file(bucket, object_name, file_name,Config=config)
    except ClientError as e:
        logging.error(e)
        return False
    return True
download_file_concurrently("./downloaded_files/sample_file.txt", bucket_name, "sample_file.txt")

# create presigned URL
def create_presigned_url(bucket, object_name, expiration=3600):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket,
            'Key': object_name},
            ExpiresIn=expiration)
    except ClientError as e:
        logging.error(e)
        return None
    return response

responseObject = create_presigned_url(bucket_name,"sample_file.txt")
print(responseObject)

# creating presigned URL for file upload
def create_presigned_upload_url(bucket_name, object_name,
                          fields=None, conditions=None, expiration=3600):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.generate_presigned_post(bucket_name,
                                                     object_name,
                                                     Fields=fields,
                                                     Conditions=conditions,
                                                     ExpiresIn=expiration)
    except ClientError as e:
        logging.error(e)
        return None
    return response

uplodable_file_name = "./sample_file_3.txt"
uploadOject = create_presigned_upload_url(bucket_name,"sample_file_3.txt")
print(uploadOject)
with open(uplodable_file_name, 'rb') as f:
     files = {'file': (uplodable_file_name, f)}
     http_response = requests.post(uploadOject['url'], data=uploadOject['fields'], files=files)
print(f'File upload HTTP status code: {http_response.status_code}')

# changing object permission
def change_object_permission(bucket, object_name, permission):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.put_object_acl(Bucket=bucket, 
                  Key=object_name, 
                  ACL=permission)
    except ClientError as e:
        logging.error(e)
        return None
    return response

change_object_permission(bucket_name, "sample_file_3.txt", "private")