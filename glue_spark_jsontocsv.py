import sys
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession

# Valid check on column
def has_column(df, col):
    try:
        df[col]
        return True
    except AnalysisException:
        return False


# Initialize contexts and session
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session

spark = SparkSession.builder. \
    config("spark.sql.autoBroadcastJoinThreshold", "-1"). \
    config("spark.driver.maxResultSize", "10g"). \
    config("fs.s3.maxRetries" "20"). \
    config("spark.driver.memory", "20g"). \
    getOrCreate()

job = Job(glue_context)

if ('--{}'.format('partfile') in sys.argv):
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'RK_ID', 'INPUT_DATENUM', 'OUTPUT_DATENUM', 'partfile'])
    s3_read_path = "s3://com-fngn-prod-datalake-raw/rni/" + "recordkeeperid=" + args['RK_ID'] + "/planownerid=" + args[
        'partfile'] + "*/" + "datenum=" + args['INPUT_DATENUM'] + "/"
else:
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'RK_ID', 'INPUT_DATENUM', 'OUTPUT_DATENUM'])
    s3_read_path = "s3://com-fngn-prod-datalake-raw/rni/" + "recordkeeperid=" + args[
        'RK_ID'] + "/planownerid=*/" + "datenum=" + args['INPUT_DATENUM'] + "/"

job.init(args['JOB_NAME'], args)

# Parameters
s3_write_path = "s3://com-fngn-prod-dataeng/raw/" + args['OUTPUT_DATENUM'] + "/rni/rni_user/"

# Log starting time
dt_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("Start time:", dt_start)

inputDF = spark.read.json(s3_read_path)

print("After inputDF  ")

transformedDF = inputDF.select(
    "userId",
    "recordKeeperId",
    "planOwnerId",
    "serviceTier",
    "initSalary",
    "increasedSavings",
    "programNumber",
    "age",
    "currentRiskValue",
    "currentExpectedReturn",
    "stoplightData.companyStockStoplight",
    "stoplightData.diversificationStoplight",
    "stoplightData.initialSavingsStoplight",
    "stoplightData.initialRetirementIncomeStoplight",
    "stoplightData.initialInvestmentStoplight",
    "stoplightData.initialCompanyStockStoplight",
    "stoplightData.investmentStoplight",
    "lastRkClickThroughDate",
    col("lastUpdated").alias("json_lastUpdated"),
    "marketSigma",
    "retirementAge",
    "stoplightData.registrationInvestmentStoplight",
    "stoplightData.registrationSavingsStoplight",
    "stoplightData.registrationRetirementIncomeStoplight",
    "stoplightData.retirementIncomeStoplight",
    "stoplightData.riskStoplight",
    "stoplightData.savingsStoplight",
    "salary",
    "savings",
    "totalLcfAllocationPct",
    "stoplightData.tdfStoplight",
    "userStatus",
    when(f.size("personalizationData") > 0, False).otherwise(True).alias("personalizationlistempty"),
    col("stoplightData.lastUpdated").alias("stoplightDatalastUpdated"),
    col("stoplightData.stoplightLastUpdated").alias("stoplightDatastoplightLastUpdated")
)

print("After transformedDF ")

if has_column(inputDF, 'initContribPct.double'):
    initcontribDF = inputDF.select("userId", "planOwnerId", "recordKeeperId",
                                   col("initContribPct.double").alias("initContribPct"))
else:
    initcontribDF = inputDF.select("userId", "planOwnerId", "recordKeeperId",
                                   col("initContribPct").alias("initContribPct"))

if has_column(inputDF, 'stoplightData.outsideAccountHoldings.assetId'):
    outsideAccountHoldingsDF = inputDF.select("userId", "planOwnerId", "recordKeeperId",
                                              when(f.size("stoplightData.outsideAccountHoldings") > 0, False).otherwise(
                                                  True).alias(
                                                  "outsideaccountholdingslistempty"))
else:
    outsideAccountHoldingsDF = inputDF.select("userId", "planOwnerId", "recordKeeperId").withColumn(
        "outsideaccountholdingslistempty", lit("True"))

# Data frame for stoplightData
if has_column(inputDF, 'stoplightData.companyStockStoplightData.unrestrictedCompanyStockPercent'):
    stoplightDataDF = inputDF.select("userId", "planOwnerId", "recordKeeperId", col(
        "stoplightData.companyStockStoplightData.unrestrictedCompanyStockPercent").alias(
        "unrestrictedCompanyStockPercent"))
else:
    stoplightDataDF = inputDF.select("userId", "planOwnerId", "recordKeeperId",
                                     col("stoplightData.companyStockStoplightData").alias(
                                         "unrestrictedCompanyStockPercent"))

if has_column(inputDF, 'enrollmentData.enrollmentEvents.lastServiceTierChange'):
    # Data frame for enrollmentData
    enrollmentDF = inputDF.withColumn("lastServiceTierChange",
                                      f.explode("enrollmentData.enrollmentEvents.lastServiceTierChange")). \
        withColumn("serviceTier_new", f.explode("enrollmentData.enrollmentEvents.serviceTier")). \
        withColumn("enrollmentChannel", f.explode("enrollmentData.enrollmentEvents.enrollmentChannel")). \
        withColumn("enrollmentReason", f.explode("enrollmentData.enrollmentEvents.enrollmentReason"))

    enrollmentDF.registerTempTable("enrollmentTable")

    enrollmentTFSQL = glue_context.sql("select userId,planOwnerId,recordKeeperId,max(enrollmentReason) as enrollmentReason,max(enrollmentChannel) as enrollmentChannel from ( select userId,planOwnerId,recordKeeperId,enrollmentReason,enrollmentChannel,lastServiceTierChange,serviceTier_new, \
                                    rank() over(partition by enrollmentReason ,enrollmentChannel order by lastServiceTierChange DESC) rnk \
                                    from enrollmentTable where serviceTier_new ='ma'  \
                                    ) where rnk=1 group by userId,planOwnerId,recordKeeperId ")
else:
    noenrollmentDF = inputDF.select("userId", "planOwnerId", "recordKeeperId").withColumn("enrollmentReason",
                                                                                          lit(None).cast(
                                                                                              StringType())).withColumn(
        "enrollmentChannel", lit(None).cast(StringType())).dropDuplicates()

    noenrollmentDF.registerTempTable("enrollmentTable")

    enrollmentTFSQL = spark.sql("select userId,planOwnerId,recordKeeperId,max(enrollmentReason) as enrollmentReason ,\
                                        max(enrollmentChannel) as enrollmentChannel from enrollmentTable  group by userId,planOwnerId,recordKeeperId")

if 'accounts' in inputDF.columns:
    # Data frame for accounts
    accountsDF = inputDF.withColumn("incomeobjectiveiexists",
                                    f.explode(f.array("accounts.*").getField("investmentObjective"))).withColumn(
        "payoutAmount", f.explode(f.array("accounts.*").getField("payoutAmount")))

    accountsDF.registerTempTable("accountsTable")

    accountsTFSQL = glue_context.sql("select userId,planOwnerId,recordKeeperId,incomeobjectiveiexists,sum(payoutAmount) as sumpayoutamount \
                                    from accountsTable where incomeobjectiveiexists ='I' \
                                    group by userId,planOwnerId,recordKeeperId,incomeobjectiveiexists ")

    joinDF = transformedDF.join(enrollmentTFSQL, on=['userId', 'planOwnerId', 'recordKeeperId'], how='left').join(
        accountsTFSQL, on=['userId', 'planOwnerId', 'recordKeeperId'],
        how='left').join(stoplightDataDF,
                         on=['userId', 'planOwnerId', 'recordKeeperId'],
                         how='left').join(
        initcontribDF, on=['userId', 'planOwnerId', 'recordKeeperId'], how='left').join(outsideAccountHoldingsDF,
                                                                                        on=['userId', 'planOwnerId',
                                                                                            'recordKeeperId'],
                                                                                        how='left')
else:
    nonaccountsDF= inputDF.select('userId','planOwnerId','recordKeeperId').withColumn("incomeobjectiveiexists", lit(None).cast(StringType())).withColumn("sumpayoutamount",lit(None).cast(StringType()))

    joinDF = transformedDF.join(enrollmentTFSQL, on=['userId', 'planOwnerId', 'recordKeeperId'], how='left'). \
        join(nonaccountsDF, on=['userId', 'planOwnerId', 'recordKeeperId'], how='left'). \
        join(stoplightDataDF, on=['userId', 'planOwnerId', 'recordKeeperId'], how='left').join(initcontribDF,  on=['userId', 'planOwnerId', 'recordKeeperId'], how='left').join(outsideAccountHoldingsDF, on=['userId', 'planOwnerId', 'recordKeeperId'], how='left')

print("After joinDF")

finalDF = joinDF.select(
    "userId",
    "recordKeeperId",
    "planOwnerId",
    "enrollmentReason",
    "enrollmentChannel",
    "personalizationlistempty",
    "outsideaccountholdingslistempty",
    "age",
    "salary",
    "userStatus",
    "lastRkClickThroughDate",
    "retirementAge",
    f.when(joinDF.incomeobjectiveiexists == 'I', True).otherwise(False).alias("investmentobjectiveiexists"),
    "sumpayoutamount",
    "serviceTier",
    "initContribPct",
    "initSalary",
    "increasedSavings",
    "initialInvestmentStoplight",
    "initialSavingsStoplight",
    "initialRetirementIncomeStoplight",
    "initialCompanyStockStoplight",
    "registrationInvestmentStoplight",
    "registrationSavingsStoplight",
    "registrationRetirementIncomeStoplight",
    "investmentStoplight",
    "savingsStoplight",
    "retirementIncomeStoplight",
    "riskStoplight",
    "diversificationStoplight",
    "companyStockStoplight",
    "tdfStoplight",
    "savings",
    "currentRiskValue",
    "currentExpectedReturn",
    "marketSigma",
    "unrestrictedCompanyStockPercent",
    "totalLcfAllocationPct",
    "json_lastUpdated",
    "stoplightDatalastUpdated",
    "stoplightDatastoplightLastUpdated"
).dropDuplicates()

print("After finalDF ")

# Create just 1 partition, because there is so little data
outputDFTransform = finalDF.repartition(1)

print("Started writing the file in Output folder")

# Convert back to dynamic frame
dynamic_frame_write = DynamicFrame.fromDF(outputDFTransform, glue_context, "dynamic_frame_write")

print("After dynamic_frame_write ")

# Write data back to S3
glue_context.write_dynamic_frame.from_options(frame=dynamic_frame_write,
                                              connection_type="s3",
                                              connection_options={"path": s3_write_path},
                                              format="csv")

job.commit()

print("Job Completed Successfully")

# Log end time
dt_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("End time:", dt_end)

print("File rename Started")

import boto3

client = boto3.client('s3')

BUCKET_NAME = "com-fngn-prod-dataeng"
PREFIX = "raw/" + args['OUTPUT_DATENUM'] + "/rni/rni_user/"

response = client.list_objects(
    Bucket=BUCKET_NAME,
    Prefix=PREFIX,
)
name = response["Contents"][0]["Key"]
new_name = args['RK_ID'] +"_" +args['OUTPUT_DATENUM'] + ".csv"
print(name)
print(new_name)
copy_source = {'Bucket': BUCKET_NAME, 'Key': name}
copy_key = PREFIX + new_name
print(copy_source)
print(copy_key)
# client.copy(CopySource=copy_source, Bucket=BUCKET_NAME, Key=copy_key)
# client.delete_object(Bucket=BUCKET_NAME, Key=name)
