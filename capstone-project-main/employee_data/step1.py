import sys
import boto3
import os
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col

# Glue job setup
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Bucket and path setup
bucket = "poc-bootcamp-capstone-project-group2"
bronze_prefix = "bronze/step-1/"
silver_prefix = "silver/employee_table/"
gold_prefix = "gold/processed/"  # updated to store final Parquet output

# Current date for partitioning
today = datetime.utcnow()
year = today.strftime('%Y')
month = today.strftime('%m')
day = today.strftime('%d')
today_str = today.strftime('%Y-%m-%d')

# S3 path for silver partition
partition_path = f"{silver_prefix}year={year}/month={month}/day={day}/"
input_partition_path = f"s3://{bucket}/{partition_path}"
gold_output_path = f"s3://{bucket}/{gold_prefix}/"

# List and relocate today's raw files
s3 = boto3.client("s3")
objects = s3.list_objects_v2(Bucket=bucket, Prefix=bronze_prefix).get("Contents", [])

for obj in objects:
    key = obj["Key"]
    if "year=" in key or key.endswith("/"):
        continue  # Skip already moved files or folders

    filename = os.path.basename(key)
    new_key = f"{partition_path}{filename}"

    # Copy to partitioned path, then delete original
    s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=new_key)
    s3.delete_object(Bucket=bucket, Key=key)

# Read all partitioned files for today
try:
    df = spark.read.option("header", True).csv(input_partition_path)
except Exception as e:
    print(f"Failed to read input partition: {e}")
    sys.exit(1)

# Clean the data
df_clean = df.select("name", "age", "emp_id") \
    .dropna(subset=["emp_id", "name","age"]) \
    .filter(col("age") > 0) \
    .dropDuplicates(["emp_id" ,"name" ,"age"])

# Write cleaned data to Parquet files in gold folder
df_clean.write \
    .mode("append") \
    .format("parquet") \
    .save(gold_output_path)

print(f"Successfully processed and saved Parquet data for {today_str} to gold path.")
job.commit()

