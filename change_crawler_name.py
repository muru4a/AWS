import boto3,logging,sys
from datetime import date, timedelta,datetime
from dateutil.relativedelta import relativedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

database_name = "fngn-dataeng-eventbus"
table_name = "household_job_updated"
new_table_name = "household_job_updated"

client = boto3.client("glue", region_name='us-west-1')
response = client.get_table(DatabaseName=database_name, Name=table_name)
#response1=client.get_partitions(DatabaseName=database_name, TableName=table_name)
table_input = response["Table"]
table_input["Name"] = new_table_name
#partitions_values = response1['Values']
print(table_input)
print(table_input["Name"])
#print(response1)


# Delete keys that cause create_table to fail
#table_input.pop("CreatedBy")
table_input.pop("CreateTime")
table_input.pop("UpdateTime")
table_input.pop("IsRegisteredWithLakeFormation")
table_input.pop("DatabaseName")


#client.create_table(DatabaseName=database_name, TableInput=table_input)

logger.info("Fetching table info for {}.{}".format(database_name, table_name))
try:
    response = client.get_table(
        #CatalogId=l_catalog_id,
        DatabaseName=database_name,
        Name=table_name
    )
except Exception as error:
    logger.error("Exception while fetching table info for {}.{} - {}"
                 .format(database_name, table_name, error))
    sys.exit(-1)

# Parsing table info required to create partitions from table
input_format = response['Table']['StorageDescriptor']['InputFormat']
output_format = response['Table']['StorageDescriptor']['OutputFormat']
table_location = response['Table']['StorageDescriptor']['Location']
serde_info = response['Table']['StorageDescriptor']['SerdeInfo']
partition_keys = response['Table']['PartitionKeys']

print(input_format)
print(output_format)
print(table_location)
print(serde_info)
print(partition_keys)

input_list = []  # Initializing empty list
#today = datetime.utcnow().date()
start_date = date(2019,12,10)
days_back = start_date - relativedelta(days=20)
delta = start_date - days_back
print(delta.days)
logger.info("Partitions to be created from {}".format(start_date))

for i in range(delta.days):
    dt = str(days_back + timedelta(i))
    print(dt)
    part_location = "{}date={}/".format(table_location, dt)
    print(part_location)
    input_dict = {
        'Values': [
            dt
        ],
        'StorageDescriptor': {
            'Location': part_location,
            'InputFormat': input_format,
            'OutputFormat': output_format,
            'SerdeInfo': serde_info
        }
    }

    input_list.append(input_dict.copy())

print(input_list)

#for each_input in input_list:
#    print(each_input)
create_partition_response = client.batch_create_partition(
        DatabaseName=database_name,
        TableName=new_table_name,
        PartitionInputList=input_list
    )

#paginator = client.get_paginator('get_partitions')
#response = paginator.paginate(
#    DatabaseName=database_name,
#    TableName=table_name
#)

#partitions = list()
#for page in response:
#    for p in page['Partitions']:
#        partitions.append(p.copy())
