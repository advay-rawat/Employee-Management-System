import sys
import boto3
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, current_timestamp, row_number, when, sum, year, month, dayofweek
)
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField, StringType, DateType

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 paths
bucket_name = "poc-bootcamp-capstone-project-group2"
bronze_path = f"s3://{bucket_name}/bronze/employee_leave_data/"
gold_path = f"s3://{bucket_name}/gold/employee_leave_data/"
emp_time_data_path = f"s3://{bucket_name}/gold/employee-timeframe-opt/status=ACTIVE/"
holiday_calendar_path = f"s3://{bucket_name}/silver/employee_leave_calendar/"

# Define schema
leave_schema = StructType([
    StructField("emp_id", StringType(), True),
    StructField("date", DateType(), True),
    StructField("status", StringType(), True)
])

# Read today's leave data
today_df = spark.read.schema(leave_schema).option("header", True).csv(bronze_path)

# Remove weekends (1 = Sunday, 7 = Saturday)
today_df = today_df.filter(~dayofweek(col("date")).isin([1, 7]))

# Load holiday calendar and filter out matching leave records
holiday_df = spark.read.option("header", True).parquet(holiday_calendar_path)
holiday_df = holiday_df.select("date").distinct()

# Remove employee leave dates that match holiday dates
today_df = today_df.join(holiday_df, on="date", how="left_anti")

# Read active employees
emp_time_df = spark.read.parquet(emp_time_data_path)

# Keep only ACTIVE employees
today_df = today_df.join(emp_time_df, "emp_id", 'left_semi')

# Add ingestion metadata
today_date = datetime.utcnow().strftime('%Y-%m-%d')
today_df = today_df.withColumn("ingest_date", lit(today_date)) \
                   .withColumn("ingest_timestamp", current_timestamp())

# Combine with historical data if exists
try:
    historical_df = spark.read.parquet(gold_path)
    combined_df = historical_df.unionByName(today_df)
    print("gold data found and combined.")
except Exception:
    print("No existing GOld data found.")
    combined_df = today_df

# Count status per employee and date
status_count_df = combined_df.withColumn(
    "is_cancelled", when(col("status") == "CANCELLED", lit(1)).otherwise(lit(0))
).withColumn(
    "is_active", when(col("status") == "ACTIVE", lit(1)).otherwise(lit(0))
).groupBy("emp_id", "date").agg(
    sum("is_cancelled").alias("cancelled_count"),
    sum("is_active").alias("active_count")
)

# Decide final status
final_status_df = status_count_df.withColumn(
    "final_status",
    when(col("cancelled_count") >= col("active_count"), lit("CANCELLED")).otherwise(lit("ACTIVE"))
)

# Filter rows that match final status
filtered_df = combined_df.join(
    final_status_df.select("emp_id", "date", "final_status"),
    on=["emp_id", "date"]
).filter(
    col("status") == col("final_status")
)

# Deduplicate using latest timestamp
window_spec = Window.partitionBy("emp_id", "date").orderBy(col("ingest_timestamp").desc())

deduped_df = filtered_df.withColumn("row_num", row_number().over(window_spec)) \
                        .filter(col("row_num") == 1) \
                        .drop("row_num", "final_status")

# Add partition columns
deduped_df = deduped_df.withColumn("year", year(col("date"))) \
                       .withColumn("month", month(col("date")))

# Write to Silver Layer (Gold path in your naming)
deduped_df.write.mode("overwrite").partitionBy("year", "month").parquet(gold_path)

# --- Write to PostgreSQL on EC2 ---
postgres_url = "jdbc:postgresql://54.165.21.137:5432/capstone_project2"
postgres_properties = {
    "user": "postgres",
    "password": "8308",
    "driver": "org.postgresql.Driver"
}

deduped_df.write \
    .jdbc(url=postgres_url, table="employee_leave_data", mode="overwrite", properties=postgres_properties)

# Commit job
job.commit()
