import boto3,logging,sys
from datetime import datetime,timedelta

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    s3 = boto3.resource('s3', region_name='us-west-1')
    s3_client = boto3.client('s3')

    source_bucket_name = 'com-fngn-finr-dataeng-heap'
    dest_bucket_name = 'com-fngn-finr-dataeng-sparkmuru'

    source_bucket = s3.Bucket(source_bucket_name)
    dest_bucket = s3.Bucket(dest_bucket_name)

    temp_date = datetime.today() -timedelta(days=1)
    yesterday_date=temp_date.strftime('%Y%m%d')


    response = s3_client.list_objects_v2(Bucket=source_bucket_name,Prefix='manifests')
    all = response['Contents']
    latest = max(all, key=lambda x: x['LastModified'])
    source_prefix1=str(latest['Key'].split('/')[1].split('.')[0] + '/')
    logger.info(latest['Key'].split('/')[1].split('.')[0])
    logger.info(source_prefix1)

    result = s3_client.list_objects(Bucket=source_bucket_name, Prefix=source_prefix1, Delimiter='/')
    for o in result.get('CommonPrefixes'):
        logger.info(o.get('Prefix'))
        new_prefix=o.get('Prefix')
        dest_prefix=str('heap'+'/'+ o.get('Prefix').split('/')[1]+'/datenum='+yesterday_date+'/')
        logger.info(dest_prefix)
        for obj in source_bucket.objects.filter(Prefix=new_prefix):
            copy_source = {'Bucket': source_bucket_name,
                           'Key': obj.key}
            logger.info(copy_source)
            try:
                new_key = obj.key.replace(new_prefix, dest_prefix, 1)
                logger.info(new_key)
                new_obj = dest_bucket.Object(new_key)
                logger.info(new_obj)
                new_obj.copy(copy_source, ExtraArgs={'ServerSideEncryption': 'AES256'})
                # dest_bucket.copy(copy_source,new_key,ExtraArgs={'ServerSideEncryption': 'AES256'})
            except Exception as error:
                logger.error("Exception while fetching table info for {} - {}"
                             .format(source_bucket, error))
                sys.exit(-1)
