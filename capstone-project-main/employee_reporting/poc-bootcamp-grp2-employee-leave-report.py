import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import DateType, StructType, StructField, StringType, TimestampType
from datetime import datetime

# Initialize Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# PostgreSQL connection details
pg_url = "jdbc:postgresql://54.165.21.137:5432/capstone_project2"
pg_properties = {
    "user": "postgres",
    "password": "8308",
    "driver": "org.postgresql.Driver"
}
pg_table2 = "employee_upcoming_leaves_count_final_table"


# ==== Read data ====
calendar_df = spark.read.parquet("s3://poc-bootcamp-capstone-project-group2/silver/employee_leave_calendar/")
leave_df = spark.read.parquet("s3://poc-bootcamp-capstone-project-group2/gold/employee_leave_data/")

# ==== Date preparation ====
today = datetime.strptime("2024-05-01", "%Y-%m-%d").date()
today_str = today.strftime('%Y-%m-%d')
end_of_year_str = f"{today.year}-12-31"

# Clean and cast date columns
calendar_df = calendar_df.withColumn("date", col("date").cast(DateType())).filter(col("date").isNotNull())
leave_df = leave_df.withColumn("leave_date", col("date").cast(DateType())).filter(col("date").isNotNull())

# Generate working day range
date_range = spark.sql(f"SELECT explode(sequence(to_date('{today_str}'), to_date('{end_of_year_str}'))) AS date")
weekends = date_range.withColumn("day_of_week", dayofweek("date")) \
                     .filter(col("day_of_week").isin([1, 7])) \
                     .select("date")

non_working_days = calendar_df.select("date").union(weekends).distinct()
working_days = date_range.join(non_working_days, on="date", how="left_anti")

total_working_days = working_days.count()
print(f"Total working days from {today_str} to {end_of_year_str}: {total_working_days}")

# ==== Leave filtering and analysis ====
future_leaves = leave_df \
    .filter((col("leave_date") >= lit(today_str)) & (col("leave_date") < lit(end_of_year_str))) \
    .dropDuplicates(["emp_id", "leave_date"]) \
    .join(working_days, leave_df.leave_date == working_days.date, "inner") \
    .select("emp_id", "leave_date")

leave_counts = future_leaves.groupBy("emp_id") \
    .agg(countDistinct("leave_date").alias("upcoming_leaves_count"))

flagged = leave_counts.withColumn(
    "leave_percent", (col("upcoming_leaves_count") / lit(total_working_days)) * 100
).withColumn(
    "flagged", when(col("leave_percent") > 8, "Yes").otherwise("No")
)

# ==== Final flagged info ====
final_flagged_leave_info = flagged.filter(col("flagged") == "Yes") \
    .select("emp_id", "upcoming_leaves_count") \
    .withColumn("run_date", lit(today_str))

# ==== Write to S3 (Parquet) ====
final_flagged_leave_info.write.mode("overwrite").partitionBy("run_date") \
    .parquet("s3://poc-bootcamp-capstone-project-group2/gold/employee_leaves_count_info/")

# ==== Write to PostgreSQL ====
final_flagged_leave_info.write \
    .jdbc(pg_url, table=pg_table2, mode="overwrite", properties=pg_properties)

# ==== Complete Job ====
job.commit()
