import pandas as pd
import boto3
import logging
import traceback
import json
import re
import pymongo
from pymongo import MongoClient
from decouple import config
from datetime import datetime, timedelta
from urllib.parse import urlparse

# Disable 'setting with copy' warning
pd.options.mode.chained_assignment = None

logger = logging.getLogger(__name__)

STAGING_DOMAINS = ['master.mwstream.com', 'studio.mwstream.com']
BETA_DOMAINS = ['beta-studio.mwstream.com', 'beta-library.mwstream.com']
PRODUCTION_DOMAINS = ['studio.masterwizr.com', 'stream.masterwizr.com']

# Boto3 clients
client = boto3.client('s3')
session = boto3.session.Session()

def download_to_mongo():
    bucket = config('S3_BUCKET_NAME')
    amplitude_data_path = config('DATA_PATH')
    folder = 'amplitude/'
    date_today = get_today_date()
    path = f'{folder}{amplitude_data_path}_{date_today}' # fetch objects from current day
    delimiter = '/'
    dfs = []
    temp = pd.DataFrame()
    data_dict = []

    try:
        json_files = client.list_objects(Bucket=bucket, Prefix=path, Delimiter=delimiter)

        for obj in json_files['Contents']:
            obj = client.get_object(Bucket=bucket, Key=obj['Key'])
            df = pd.read_json(obj['Body'], lines=True) # read data frame from json file
            dfs.append(df) # append the data frame to the list
        temp = pd.concat(dfs, ignore_index=True) # concatenate all the data frames in the list
        data_dict = json.loads(temp.T.to_json()).values()
        process_and_upload_data(data_dict)
        return {'statusCode': 200, 'body': {'message': 'success'}}
    except Exception as e:
        logging.error('Error while handling lambda event')
        logging.error(traceback.print_exc())
        return {'statusCode': 500, 'body': {'message': 'failed'}}

def process_and_upload_data(data_dict):
    # Clean data
    cleaned_data_df = clean_data(data_dict)

    # Separate the different environments
    staging_df, beta_df, production_df = separate_environments(cleaned_data_df)

    # Upload separated data to MongoDB
    upload_to_mongo(staging_df, 'staging')
    upload_to_mongo(production_df, 'production')
    upload_to_mongo(beta_df, 'beta')


def clean_data(data_dict):
    # Normalize the json data to properly read in nested data
    data_df = pd.json_normalize(list(data_dict), sep='_')

    data_df.rename(columns=lambda x: x.strip('$'), inplace=True) # strip the $ at start of column names

    cols_to_drop = ['app', 'amplitude_event_type', 'device_type', 'device_carrier', 'device_brand', 'device_family', 
                    'device_manufacturer', 'location_lat', 'location_lng', 'dma', 'idfa', 'adid', 'library', 'platform',
                    'paying', 'groups', 'group_properties', 'start_version', 'version_name', 'sample_rate',
                    'is_attribution_event', 'amplitude_attribution_ids', 'event_properties_organizationId', 'user_properties_organizationId'
                    ]

    cleaned_data_df = data_df.drop(columns=cols_to_drop, errors='ignore')

    # Make all columns have uniform naming convention (snake_case)
    cleaned_data_df = clean_column_names(cleaned_data_df)

    return cleaned_data_df

def clean_column_names(df):
    col_names = df.columns
    new_col_names = []
    pattern = re.compile(r'(?<!^)(?=[A-Z])')
    for name in col_names:
        name = pattern.sub('_', name).lower() # make it snake case
        name = re.sub(r'\.', '_', name) # replace . with _
        new_col_names.append(name)
    df.columns = new_col_names
    return df

def separate_environments(df):
    df = create_domain(df)

    staging_df = df[df['domain'].isin(STAGING_DOMAINS)]
    beta_df = df[df['domain'].isin(BETA_DOMAINS)]
    production_df = df[df['domain'].isin(PRODUCTION_DOMAINS)]

    return staging_df, beta_df, production_df

def apply_domain(row):
    if type(row['event_properties_url']) == str:
        domain = urlparse(row['event_properties_url']).netloc
    else:
        domain = None
    return domain

def create_domain(df):
    df['domain'] = df.apply(apply_domain, axis=1)
    return df

def upload_to_mongo(df, collection_name):
    user = config('MONGO_USER')
    password = config('MONGO_PASSWORD')
    try:
        client = pymongo.MongoClient(f'mongodb+srv://{user}:{password}@cluster0.yhpvw.mongodb.net/masterwizr-data-db?retryWrites=true&w=majority')
        db = client['masterwizr-data-db']
        logging.info('Connected to Mongo')
        records = json.loads(df.T.to_json()).values()
        collection = db[collection_name]
        collection.create_index([('insert_id', pymongo.ASCENDING)], name='insert_id', unique=True)
        collection.insert_many(records)
    except pymongo.bulk.BulkWriteError as bwe:
        for err in bwe.details['writeErrors']:
            if int(err['code']) == 11000:
                pass
            else:
                logging.error(err['errmsg'])
    except Exception as e:
        logging.error(traceback.print_exc())
        return {'statusCode': 500, 'body': {'message': 'Failed to connect to mongo'}}

def get_today_date():
    return (datetime.now() - timedelta(days=0)).strftime('%Y-%m-%d')