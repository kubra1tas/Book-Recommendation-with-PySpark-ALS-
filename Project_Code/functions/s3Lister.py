#! /usr/bin/env python
# -*- coding: UTF-8 -*-
#dir + "/Raw_Data/"
# @updatedDate: 10.02.2020
# @version: 1.0.0
#

import json
import boto3
import pprint
import csv
import boto3 as boto3
import os

dir = os.getcwd()
path = (dir + "/Raw_Data/")


def get_matching_s3_keys(bucket, prefix='', suffix=''):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    s3 = boto3.client('s3')

    kwargs = {'Bucket': bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix
    import pprint
    while True:

        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3.list_objects_v2(**kwargs)
        pprint.pprint(resp)
        if resp['Contents'] != None:
            for obj in resp['Contents']:
                key = obj['Key']
                if key.startswith(prefix) and key.endswith(suffix):
                    yield key

        # The S3 API is paginated, returning up to 1000 keys at a tme.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

acces_key = #############
secret_key = ################

def sess():

    session = boto3.Session(
        aws_access_key_id=acces_key,
        aws_secret_access_key=secret_key)

    s3 = session.resource('s3')
    bck = s3.Bucket('babil--click.stream.data')
    prefix='prod/Data/2020/07'

    with open((path + '/JulyData.csv'), 'w', newline='') as file:   #  MUST BE CHANGED WITH EVERY NEW DOCUMENT !!!!
        #wrt = csv.writer(file, delimiter=',')

        for obj in bck.objects.filter(Prefix=prefix):
             #print('{0}:{1}'.format(bck.name, obj.key))
             data = obj.get()['Body'].read()
             new = data
             data = data.decode("utf-8")
             json_data = json.loads('[' + data.replace('}{', '},{').replace('} {', '},{') + ']')



             data = data.split("{")


             for i in data:

                 file.writelines(i + '\n')
             file.writelines('\n')
