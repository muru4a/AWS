#!/usr/bin/env python
import boto3
import logging
import json
import os
from datetime import datetime,timedelta

def handler(event, context):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(stream=None)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

   # Initilize S3 client
    s3resource = boto3.resource('s3', region_name='us-west-1')
    s3client = boto3.client('s3')

    # define source and target 
    source_bucket_name = os.environ['source_bucket']
    dest_bucket_name = os.environ['dest_bucket']

    # Check target bucket has key heap/manifets/ , if not create 

    results = s3client.list_objects_v2(Bucket=dest_bucket_name, Prefix='heap/manifests/')
    if not 'Contents' in results:
        s3client.put_object(Bucket=dest_bucket_name, Key='heap/manifests/')

    # Get the list of syncs that need to processed to target bucket

    results = s3client.list_objects_v2(Bucket=source_bucket_name, Prefix='manifests/')
    src_sync_list = [ sync_info['Key'] for sync_info in results['Contents'] if 'json' in sync_info['Key'] ]
    src_sync_list = [ sync.replace('manifests/', '').replace('.json', '') for sync in src_sync_list ]
    results = s3client.list_objects_v2(Bucket=dest_bucket_name, Prefix='heap/manifests/')
    dest_sync_list = [ sync_info['Key'] for sync_info in results['Contents'] if 'json' in sync_info['Key'] ]  if 'Contents' in results else []
    dest_sync_list = [ sync.replace('heap/manifests/', '').replace('.json', '') for sync in dest_sync_list ]
    diff_sync_list = [ sync for sync in src_sync_list if sync not in dest_sync_list ]


    # Now process each sync in diff_sync_list to destination bucket

    for sync in diff_sync_list:

        logger.info("Processing sync: {}".format(sync))

        # Check the sync application is main/fpt and modified time
        obj = s3client.get_object(Bucket=source_bucket_name, Key='manifests/{}.json'.format(sync))
        sync_properties = json.loads(obj.get('Body').read().decode('utf-8'))
        partition_datenum = (obj.get('LastModified') - timedelta(days=1)).strftime('%Y%m%d')
        
        col_count_pageviews = [len(sync_properties.get('tables')[index].get('columns')) for index in range(len(sync_properties.get('tables'))) if sync_properties.get('tables')[index].get('name') == 'pageviews'][0]
        col_count_users = [len(sync_properties.get('tables')[index].get('columns')) for index in range(len(sync_properties.get('tables'))) if sync_properties.get('tables')[index].get('name') == 'users'][0]

        application = ''
        if col_count_pageviews == 39 and col_count_users == 9:
            application = 'fpt'
        elif col_count_pageviews == 33 and col_count_users == 13:
            application = 'main'
        elif col_count_pageviews == 33 and col_count_users == 6:
            application = 'lpw'
        else:
            logger.error("Change detected in Column count. Please check column list")
        
        results = s3client.list_objects_v2(Bucket=source_bucket_name, Prefix=sync+'/')
        
        heap_contents = [ content['Key'].replace(sync+'/', '') for content in results['Contents'] if content['Key'] != sync + '/' and content['Key'] != sync + '/property_definitions.json']
    
        
        for heap_content in heap_contents:
            copy_source = {'Bucket': source_bucket_name, 'Key': '{}/{}'.format(sync, heap_content)}
            toplevel_key = heap_content.split('/')[0]
            dest_key = 'heap/{}/{}/datenum={}/{}'.format(application, 
                                    toplevel_key, 
                                    partition_datenum, 
                                    heap_content.replace(toplevel_key + '/', '')
                                ) 
            try:
                s3resource.meta.client.copy(copy_source, dest_bucket_name, dest_key, ExtraArgs={'ServerSideEncryption': 'AES256'})
            except Exception as error:
                logger.error("Exception while copying from {} to {} - {}".format(copy_source.get('Key'), dest_key, error))


        #Copy the Manifests file
        copy_source = {'Bucket': source_bucket_name, 'Key': 'manifests/{}.json'.format(sync)}
        dest_key = 'heap/manifests/{}.json'.format(sync)

        try:
            s3resource.meta.client.copy(copy_source, dest_bucket_name, dest_key, ExtraArgs={'ServerSideEncryption': 'AES256'})
        except Exception as error:
            logger.error("Exception while copying from {} to {} - {}".format(copy_source.get('Key'), dest_key, error))
