

import sys
import boto3
from urllib.parse import urlparse
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, row_number, lead, when, from_unixtime
from pyspark.sql.window import Window

# Initialize context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 paths
input_path = "s3://poc-bootcamp-capstone-project-group2/bronze/employee_timeframe_data/unprocessed/"
processed_prefix = "bronze/processed/"
silver_path = "s3://poc-bootcamp-capstone-project-group2/silver/employee_timeframe_data/"

# Read raw data
df = spark.read.option("header", True).csv(input_path)

# Track input files to move after processing
input_files = df.inputFiles()

# Type casting
df = df.withColumn("start_date", col("start_date").cast("long")) \
       .withColumn("end_date", col("end_date").cast("long")) \
       .withColumn("salary", col("salary").cast("long"))

# Deduplicate: keep highest salary per emp_id and start_date
window_dedupe = Window.partitionBy("emp_id", "start_date").orderBy(col("salary").desc())
df_deduped = df.withColumn("row_num", row_number().over(window_dedupe)) \
               .filter(col("row_num") == 1) \
               .drop("row_num")

# Fill missing end_dates using next start_date
window_next = Window.partitionBy("emp_id").orderBy("start_date")
df_filled = df_deduped.withColumn("next_start_date", lead("start_date").over(window_next)) \
                      .withColumn("end_date", when(col("end_date").isNull(), col("next_start_date"))
                                             .otherwise(col("end_date"))) \
                      .drop("next_start_date")

# Convert Unix timestamps to date and add status
df_transformed = df_filled.withColumn("start_date", from_unixtime("start_date").cast("date")) \
                          .withColumn("end_date", from_unixtime("end_date").cast("date")) \
                          .withColumn("status", when(col("end_date").isNull(), "ACTIVE").otherwise("INACTIVE"))

# Write to silver layer
df_transformed.write.mode("append").option("header", True).csv(silver_path)

# Move processed files to processed folder (no ListBucket permission needed)
s3 = boto3.resource("s3")

for file_path in input_files:
    parsed = urlparse(file_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')
    dest_key = key.replace("bronze/unprocessed/", processed_prefix)

    copy_source = {'Bucket': bucket, 'Key': key}
    s3.Object(bucket, dest_key).copy_from(CopySource=copy_source)
    s3.Object(bucket, key).delete()

job.commit()
