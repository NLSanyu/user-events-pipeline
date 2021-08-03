import traceback
import requests
import shutil
import zipfile
import os
import gzip
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
import logging
from decouple import config

import boto3
from botocore.client import Config

project_id = config('DATA_PATH')
base_path = "/tmp/amplitude_data/"
project_directory = base_path + project_id
extracted_json_data = base_path + "extracted/"
zip_file_path = "/tmp/amplitude_data.zip"
amplitude_key = config("AMPLITUDE_API_KEY")
amplitude_secret = config("AMPLITUDE_SECRET_KEY")

logger = logging.getLogger(__name__)


def upload_to_s3(): 
    try:
        date = get_today_date()
        print(date)
        url = f'https://amplitude.com/api/2/export?start={date}T1&end={date}T23'
        response = requests.get(url, auth=HTTPBasicAuth(amplitude_key, amplitude_secret), stream=True)
        logging.info(response.status_code)
        with open(zip_file_path, 'wb') as out_file:
            shutil.copyfileobj(response.raw, out_file)
        del response
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(base_path)

        if not os.path.exists(extracted_json_data):
            os.makedirs(extracted_json_data)

        for filename in os.listdir(project_directory):
            if not filename.endswith(".gz"):
                continue
            file_path = os.path.join(project_directory, filename)
            with gzip.open(file_path, 'rb') as f_in:
                json_file_name = filename[:-3]
                with open(extracted_json_data + json_file_name, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        upload_files_s3(extracted_json_data)
        return {'statusCode': 200, "body": {"message": "success"}}
    except Exception as e:
        logging.error("error while handling lambda event ")
        logging.error(traceback.print_exc())
        return {'statusCode': 500, "body": {"message": "Failed"}}


def get_today_date():
    return (datetime.now() - timedelta(days=0)).strftime("%Y%m%d")


def upload_files_s3(directory_path):
    logging.info("Uploading files to S3")

    bucket = config("S3_BUCKET_NAME")
    s3_client = boto3.client('s3', config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}), region_name='eu-north-1')

    for filename in os.listdir(directory_path):
        if not filename.endswith(".json"):
            continue
        file_path = os.path.join(directory_path, filename)
        s3_key = f"amplitude/{filename}"
        with open(file_path, 'rb') as data:
            s3_client.upload_fileobj(data, bucket, s3_key, ExtraArgs={'ContentType': 'application/json'})