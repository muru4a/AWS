#!/usr/bin/env python
import boto3
import logging
from datetime import timedelta

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Initilize S3 client
    s3resource = boto3.resource('s3', region_name='us-west-1')
    s3client = boto3.client('s3')

    # define source and target
    source_bucket_name = 'com-fngn-finr-dataeng-sparkmuru' #com-fngn-finr-dataeng-rni'
    dest_bucket_name = 'com-fngn-finr-dataeng-rni'

    # Check target bucket has key heap/manifets/ , if not create

    #results = s3client.list_objects_v2(Bucket=dest_bucket_name, Prefix='heap/manifests/')
    #if not 'Contents' in results:
    #    s3client.put_object(Bucket=dest_bucket_name, Key='heap/manifests/')

    # Get the list of syncs that need to processed to target bucket

    #result = s3client.list_objects(Bucket=source_bucket_name, Prefix='rni/', Delimiter='/')

    all_objects = s3client.list_objects(Bucket=source_bucket_name)
    #print(all_objects)

    results = s3client.list_objects_v2(Bucket=source_bucket_name, Prefix='rni/')
    print(results)
    src_sync_list = [sync_info['Key'] for sync_info in results['Contents']]
    print (src_sync_list)

    result = s3client.list_objects(Bucket=source_bucket_name, Prefix='rni/', Delimiter='/')
    list_folders = [x['Prefix'] for x in result['CommonPrefixes']]

    for o in list_folders:

        print ('sub folder : ', o )
        list_objects = s3client.list_objects(Bucket=source_bucket_name, Prefix=o,Delimiter="/")
        src_sync_list = [x['Prefix'] for x in list_objects['CommonPrefixes']]

        for sync in src_sync_list:
            list_files = s3client.list_objects_v2(Bucket=source_bucket_name, Prefix=sync)
            src_sync_list1 = [sync_info['Key'] for sync_info in list_files['Contents']]
            print(src_sync_list1)

            for obj in src_sync_list1:

                logger.info("Processing sync: {}".format(obj))

                obj1 = s3client.get_object(Bucket=source_bucket_name, Key='rni/')
                partition_datenum = (obj1.get('LastModified') - timedelta(days=1)).strftime('%Y%m%d')
                print(partition_datenum)

                results = s3client.list_objects_v2(Bucket=source_bucket_name, Prefix=obj)

                print(results)

                recordKeeperId= results['Prefix'].split("/")[1]
                planownerId = results['Prefix'].split("/")[2]
                file_name = results['Prefix'].split("/")[3]

                print ("{},{},{}".format(recordKeeperId,planownerId,file_name))

                toplevel_key = results['Prefix'].replace('rni/', '')
                print(toplevel_key)

                copy_source = {'Bucket': source_bucket_name, 'Key': 'rni/{}'.format(toplevel_key)}
                print(copy_source)

                dest_key = 'rni_new_latest/{}/{}/datenum={}/{}'.format(recordKeeperId,planownerId,
                                                            partition_datenum,file_name
                                                          )
                print(dest_key)

                try:
                    s3resource.meta.client.copy(copy_source, dest_bucket_name, dest_key,
                                                    ExtraArgs={'ServerSideEncryption': 'AES256'})
                except Exception as error:
                    logger.error("Exception while copying from {} to {} - {}".format(copy_source.get('Key'), dest_key, error))
