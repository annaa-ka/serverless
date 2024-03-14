import argparse
import time
import json
import os
import subprocess
import uuid
from urllib.parse import urlencode
import logging
import cv2 as cv
import boto3
import requests
import yandexcloud
from yandex.cloud.lockbox.v1.payload_service_pb2 import GetPayloadRequest
from yandex.cloud.lockbox.v1.payload_service_pb2_grpc import PayloadServiceStub

boto_session = None
storage_client = None
docapi_table = None
ymq_queue = None


def predict(net, img, h, w):
    blob = cv.dnn.blobFromImage(img, 1.0, (w, h),
                                (103.939, 116.779, 123.680), swapRB=False, crop=False)

    print('[INFO] Setting the input to the model')
    net.setInput(blob)

    print('[INFO] Starting Inference!')
    start = time.time()
    out = net.forward()
    end = time.time()
    print('[INFO] Inference Completed successfully!')

    # Reshape the output tensor and add back in the mean subtraction, and
    # then swap the channel ordering
    out = out.reshape((3, out.shape[2], out.shape[3]))
    out[0] += 103.939
    out[1] += 116.779
    out[2] += 123.680
    out /= 255.0
    out = out.transpose(1, 2, 0)

    # Printing the inference time
    print('[INFO] The model ran in {:.4f} seconds'.format(end - start))

    return out


# Source for this function:
# https://github.com/jrosebr1/imutils/blob/4635e73e75965c6fef09347bead510f81142cf2e/imutils/convenience.py#L65
def resize_img(img, width=None, height=None, inter=cv.INTER_AREA):
    dim = None
    h, w = img.shape[:2]

    if width is None and height is None:
        return img
    elif width is None:
        r = height / float(h)
        dim = (int(w * r), height)
    elif height is None:
        r = width / float(w)
        dim = (width, int(h * r))

    resized = cv.resize(img, dim, interpolation=inter)
    return resized


def process_image(image, model, output):
    net = cv.dnn.readNetFromTorch(model)
    img = cv.imread(image)
    img = resize_img(img, width=600)
    h, w = img.shape[:2]
    out = predict(net, img, h, w)
    out = cv.convertScaleAbs(out, alpha=255.0)
    cv.imwrite(output, out)


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


def download_and_presign(file_path, object_name):
    client = get_storage_client()
    bucket = os.environ['RESULTS_S3_BUCKET']
    client.upload_file(file_path, bucket, object_name)
    return client.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': object_name}, ExpiresIn=3600)


def style_transfer(event, context):
    for message in event['messages']:
        task_json = json.loads(message['details']['message']['body'])
        task_id = task_json['task_id']

        bucket = os.environ['UPLOAD_S3_BUCKET']
        file_name = task_id

        tmp_file_name = '/tmp/' + file_name + ".jpg"
        client = get_storage_client()
        client.download_file(bucket, file_name, tmp_file_name)

        object_name = 'converted-' + file_name + ".jpg"
        result_file_path = "/tmp/" + object_name
        process_image(tmp_file_name, 'image-converting/models/mosaic.t7', result_file_path)

        presigned_url = download_and_presign(result_file_path, object_name)

        get_docapi_table().update_item(
            Key={'task_id': task_id},
            AttributeUpdates={
                'status': {'Value': 'DONE', 'Action': 'PUT'},
                'url': {'Value': presigned_url, 'Action': 'PUT'}

            }
        )

    return "OK"
