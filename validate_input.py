import argparse
import time
import json
import os
import subprocess
import uuid
from urllib.parse import urlencode
import logging
import boto3
import requests
import yandexcloud
from yandex.cloud.lockbox.v1.payload_service_pb2 import GetPayloadRequest
from yandex.cloud.lockbox.v1.payload_service_pb2_grpc import PayloadServiceStub

boto_session = None
storage_client = None
docapi_table = None
ymq_queue = None

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

    # initialize boto session
    boto_session = boto3.session.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    return boto_session


def get_ymq_queue():
    global ymq_queue
    if ymq_queue is not None:
        return ymq_queue

    ymq_queue = get_boto_session().resource(
        service_name='sqs',
        endpoint_url='https://message-queue.api.cloud.yandex.net',
        region_name='ru-central1'
    ).Queue(os.environ['YMQ_QUEUE_URL'])
    return ymq_queue


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


# 10485760 - 10 Mb
def validate_input(event, context):
    bucket = os.environ['UPLOAD_S3_BUCKET']

    file_name, task_id = None, None
    for message in event['messages']:
        if 'details' in message:
            file_name = message['details']['object_id']
            task_id = file_name

    tmp_file_name = '/tmp/' + file_name + ".jpg"
    client = get_storage_client()
    client.download_file(bucket, file_name, tmp_file_name)

    file_size = os.path.getsize(tmp_file_name)
    if file_size > 10485760:
        get_docapi_table().update_item(
            Key={'task_id': task_id},
            AttributeUpdates={
                'status': {'Value': "INVALID", 'Action': 'PUT'},
            }
        )
        return "OK"

    get_docapi_table().update_item(
        Key={'task_id': task_id},
        AttributeUpdates={
            'status': {'Value':  "PROCESSING", 'Action': 'PUT'},
        }
    )

    get_ymq_queue().send_message(MessageBody=json.dumps({'task_id': task_id}))
    return "OK"
