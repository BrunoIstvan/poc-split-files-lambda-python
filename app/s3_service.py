from os import environ
import boto3

def __get_client():
    
    return boto3.client('s3')


def get_object(bucket_name, key):
    
    client = __get_client()
    return client.get_object(Bucket=bucket_name,
                             Key=key)


def move_object(bucket_origin, key_origin, bucket_destination, key_destination):
    
    client = __get_client()
    client.copy_object(Bucket=bucket_destination,
                       Key=key_destination,
                       CopySource={
                           'Bucket': bucket_origin,
                           'Key': key_origin
                       })
    delete_object(bucket_name=bucket_origin, key=key_origin)


def delete_object(bucket_name, key):
    
    client = __get_client()
    return client.delete_object(Bucket=bucket_name,
                                Key=key)


def put_object(bucket_name, key, file):
    
    client = __get_client()
    return client.put_object(Bucket=bucket_name,
                             Key=key,
                             Body=file)


def list_all_buckets():
    
    client = __get_client()
    response = client.list_buckets()
    # print(response)
    return response['Buckets']
