import json
import os
import subprocess
import uuid
from urllib.parse import urlencode

import boto3
import requests
import yandexcloud
from yandex.cloud.lockbox.v1.payload_service_pb2 import GetPayloadRequest
from yandex.cloud.lockbox.v1.payload_service_pb2_grpc import PayloadServiceStub

boto_session = None
storage_client = None
docapi_table = None


def get_boto_session():
    global boto_session
    if boto_session is not None:
        return boto_session

    # initialize lockbox and read secret value
    yc_sdk = yandexcloud.SDK()
    channel = yc_sdk._channels.channel("lockbox-payload")
    lockbox = PayloadServiceStub(channel)
    response = lockbox.Get(GetPayloadRequest(secret_id=os.environ['SECRET_ID']))

    # extract values from secret
    access_key = None
    secret_key = None
    for entry in response.entries:
        if entry.key == 'ACCESS_KEY_ID':
            access_key = entry.text_value
        elif entry.key == 'SECRET_ACCESS_KEY':
            secret_key = entry.text_value
    if access_key is None or secret_key is None:
        raise Exception("secrets required")
    print("Key id: " + access_key)

    # initialize boto session
    boto_session = boto3.session.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    return boto_session



def get_docapi_table():
    global docapi_table
    if docapi_table is not None:
        return docapi_table

    docapi_table = get_boto_session().resource(
        'dynamodb',
        endpoint_url=os.environ['DOCAPI_ENDPOINT'],
        region_name='ru-central1'
    ).Table('tasks')
    return docapi_table


def get_storage_client():
    global storage_client
    if storage_client is not None:
        return storage_client

    storage_client = get_boto_session().client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        region_name='ru-central1'
    )
    return storage_client


# API handler
# создаем в таблицу новое задание
# генерируем presigned url
def create_task():
    task_id = str(uuid.uuid4())
    get_docapi_table().put_item(Item={
        'task_id': task_id,
        'status': 'NEW',
        'url': ''
    })

    client = get_storage_client()
    bucket = os.environ['UPLOAD_S3_BUCKET']
    presigned_url = client.generate_presigned_post(bucket, task_id)
    response = {
        'statusCode': 200,
        'headers': {"content-type": "application/json"},
        'body': {
            'task_id': task_id,
            'presigned_url': presigned_url
        }
    }

    return  response


def get_task_status(task_id):
    task = get_docapi_table().get_item(Key={
        'task_id': task_id
    })
    response = {
        'statusCode': 200,
        'headers': {"content-type": "application/json"},
        'body': {
            'task_id': task_id,
            'status': task['Item']['status']
        }
    }

    if task['Item']['status'] == 'DONE':
        response['body']['url'] = task['Item']['url']

    return response


def handle_api(event, context):
    action = event['queryStringParameters']['action']
    if action == 'convert':
        return create_task()
    elif action == 'get_task_status':
        return get_task_status(event['queryStringParameters']['task_id'])
    else:
        return {"error": "unknown action: " + action}