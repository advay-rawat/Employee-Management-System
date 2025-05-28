import sys
import boto3
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, month, year, to_date, count, round, when, lit

# ==== Job initialization ====
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ==== S3 Paths ====
leave_data_path = "s3://poc-bootcamp-capstone-project-group2/gold/employee_leave_data/"
quota_data_path = "s3://poc-bootcamp-capstone-project-group2/silver/employee_leave_quota/"
output_path = "s3://poc-bootcamp-capstone-project-group2/gold/employees_80_percent_leave_used/"
txt_output_prefix = "gold/leave_alerts/"
tracking_path = "s3://poc-bootcamp-capstone-project-group2/silver/alerted_emp_ids_tracking/"

bucket = "poc-bootcamp-capstone-project-group2"
run_date_str = datetime.now().strftime("%Y-%m-%d")
current_month = 12
current_year = 2024

# ==== Read and preprocess leave data ====
employee_leaves_df = spark.read.parquet(leave_data_path)
employee_leaves_df = employee_leaves_df.withColumn("date", to_date(col("date")))

leaves_agg_df = employee_leaves_df \
    .filter((month(col("date")) < current_month) &
            (year(col("date")) == current_year) &
            (col("status") == "ACTIVE")) \
    .groupBy("emp_id") \
    .agg(count("*").alias("leaves_taken"))

# ==== Read and join with quota data ====
quota_df = spark.read.parquet(quota_data_path).filter(col("year") == current_year)

usage_df = leaves_agg_df.join(quota_df, "emp_id", "left") \
    .withColumn("leave_quota", col("leave_quota").cast("int")) \
    .withColumn("leave_percent", round((col("leaves_taken") / col("leave_quota")) * 100, 2)) \
    .select("emp_id", "leaves_taken", "leave_quota", "leave_percent")

# ==== Identify flagged employees (>80%) ====
flagged_df = usage_df.filter(col("leave_percent") > 80)
flagged_df.write.mode("overwrite").parquet(output_path)

# ==== Try reading the previously alerted emp_ids ====
try:
    existing_alerts_df = spark.read.parquet(tracking_path)
    existing_emp_ids = set(row["emp_id"] for row in existing_alerts_df.collect())
except Exception as e:
    print("No previous tracking file found or read error:", str(e))
    existing_emp_ids = set()

# ==== Identify new alerts only ====
new_alerts_df = flagged_df.filter(~col("emp_id").isin(existing_emp_ids))
new_alerts = new_alerts_df.collect()

# ==== Commit job before writing TXT files ====
job.commit()

# ==== After successful commit, write alerts using boto3 ====
if new_alerts:
    s3_client = boto3.client('s3')
    for row in new_alerts:
        try:
            emp_id = row["emp_id"]
            content = (
                f"Employee ID: {emp_id}\n"
                f"Leave Taken: {row['leaves_taken']}\n"
                f"Leave Quota: {row['leave_quota']}\n"
                f"Usage: {row['leave_percent']:.2f}%\n"
                f"Report Date: {run_date_str}\n"
            )
            s3_client.put_object(
                Bucket=bucket,
                Key=f"{txt_output_prefix}{emp_id}_{run_date_str}.txt",
                Body=content.encode("utf-8")
            )
            print(f" Alert written for: {emp_id}")
        except Exception as e:
            print(f" Failed to write alert for {row['emp_id']}: {str(e)}")

    # ==== Update tracking file only after successful TXT writes ====
    try:
        new_ids_df = spark.createDataFrame([(row["emp_id"],) for row in new_alerts], ["emp_id"])
        if existing_emp_ids:
            updated_df = existing_alerts_df.union(new_ids_df).dropDuplicates(["emp_id"])
        else:
            updated_df = new_ids_df
        updated_df.write.mode("overwrite").parquet(tracking_path)
        print(" Tracking file updated.")
    except Exception as e:
        print("Failed to update tracking file:", str(e))
else:
    print("No new employees crossed 80% usage.")

# ==== End ====
