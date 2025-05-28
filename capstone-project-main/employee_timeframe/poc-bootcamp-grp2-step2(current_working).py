import sys
import boto3
import os
import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, to_date, from_unixtime, when, row_number, lit, lead
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# S3 paths
bucket = "poc-bootcamp-capstone-project-group2"
bronze_prefix = "bronze/step-2/"
processed_path = f"s3://{bucket}/silver/employee_timeframe_processed/"
output_path = f"s3://{bucket}/gold/employee_timeframe_output/"
input_path = f"s3://{bucket}/{bronze_prefix}"

# Step 1: Define schema
schema = StructType([
    StructField("emp_id", StringType(), True),
    StructField("designation", StringType(), True),
    StructField("start_date", LongType(), True),
    StructField("end_date", DoubleType(), True),
    StructField("salary", DoubleType(), True),
])

# Step 2: Read new CSV data
df = spark.read.option("header", "true").schema(schema).csv(input_path)
if df.rdd.isEmpty():
    raise Exception(" No CSV files found or file is empty")

# Step 3: Convert timestamp columns
df = df.withColumn("start_date", to_date(from_unixtime(col("start_date").cast(DoubleType())))) \
       .withColumn("end_date", to_date(from_unixtime(col("end_date").cast(DoubleType()))))

# Step 4: Deduplicate new data by salary
dedup_window = Window.partitionBy("emp_id", "start_date", "end_date").orderBy(col("salary").desc())
df_dedup = df.withColumn("rn", row_number().over(dedup_window)).filter(col("rn") == 1).drop("rn")

# Step 5: Assign temp end_date for continuity using lead
lead_window = Window.partitionBy("emp_id").orderBy("start_date")
df_lead = df_dedup.withColumn("next_start_date", lead("start_date").over(lead_window)) \
                  .withColumn("final_end_date", when(col("end_date").isNull(), col("next_start_date")).otherwise(col("end_date"))) \
                  .drop("end_date", "next_start_date") \
                  .withColumnRenamed("final_end_date", "end_date")

# Step 6: Assign status column
df_with_status = df_lead.withColumn(
    "status", when(col("end_date").isNull(), lit("ACTIVE")).otherwise(lit("INACTIVE"))
)

# Step 7: Read existing data from Gold
try:
    all_prev_data = spark.read.parquet(output_path)
except:
    all_prev_data = spark.createDataFrame([], df_with_status.schema)

# Step 8: Filter out already present records
existing_keys = all_prev_data.select("emp_id", "start_date", "end_date").dropDuplicates()
new_records = df_with_status.alias("new").join(
    existing_keys.alias("exist"),
    on=["emp_id", "start_date", "end_date"],
    how="left_anti"
)

# Step 9: Combine all data (previous + new)
combined_df = all_prev_data.unionByName(new_records)

# Step 10: Recompute end_date and status using lead to ensure continuity
final_df = combined_df.withColumn("next_start_date", lead("start_date").over(lead_window)) \
                      .withColumn("end_date", col("next_start_date")) \
                      .drop("next_start_date") \
                      .withColumn("status", when(col("end_date").isNull(), lit("ACTIVE")).otherwise(lit("INACTIVE")))

# Step 11: Deduplicate across final dataset
final_dedup_window = Window.partitionBy("emp_id", "start_date", "end_date").orderBy(col("salary").desc())
final_df_dedup = final_df.withColumn("rn", row_number().over(final_dedup_window)) \
                         .filter(col("rn") == 1) \
                         .drop("rn")

# Step 12: Write clean final data to Gold
final_df_dedup.write.mode("overwrite").partitionBy("status").parquet(output_path)

# Step 13: Archive raw data to Silver path
today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
df.write.mode("append").option("header", "true").csv(f"{processed_path}date={today}/")

# Step 14: Move raw files from Bronze → Silver (archiving)
s3 = boto3.client("s3")
objects = s3.list_objects_v2(Bucket=bucket, Prefix=bronze_prefix).get("Contents", [])

for obj in objects:
    key = obj["Key"]
    if key.endswith("/") or "year=" in key:
        continue
    filename = os.path.basename(key)
    new_key = f"silver/employee_timeframe_processed/date={today}/{filename}"
    s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=new_key)
    s3.delete_object(Bucket=bucket, Key=key)

print(" Job completed successfully. Gold layer updated and raw files archived to Silver.")
