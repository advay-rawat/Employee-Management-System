import sys
import boto3
import os
import datetime
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, to_date, from_unixtime, when, row_number, lit, lead, current_date
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


# Fetch resolved arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize the Job with the correct parameters
job = Job(glueContext)
job.init(args['JOB_NAME'], args)  # Pass args here correctly


bucket = "poc-bootcamp-capstone-project-group2"
input_path = f"s3://{bucket}/bronze/employee-timeframe-opt/"
output_path = f"s3://{bucket}/gold/employee-timeframe-opt/"

pg_host = "54.165.21.137"
pg_port = "5432"
pg_user = "postgres"
pg_password = "8308"
pg_dbname = "capstone_project2"
pg_staging_table = "employee_timeframe_final_table_staging"
pg_target_table = "employee_timeframe_final_table"
striked_out_table = "striked_out"
# JDBC URL
jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_dbname}"

# JDBC properties
pg_properties = {
    "user": pg_user,
    "password": pg_password,
    "driver": "org.postgresql.Driver"
}

# Schema
schema = StructType([
    StructField("emp_id", StringType(), True),
    StructField("designation", StringType(), True),
    StructField("start_date", DoubleType(), True),
    StructField("end_date", DoubleType(), True),
    StructField("salary", DoubleType(), True),
])

# Read data
df = spark.read.option("header", "true").schema(schema).csv(input_path)
if df.rdd.isEmpty():
    print("NO INPUT DATA FOUND")
else :
    striked_df = spark.read.jdbc(url=jdbc_url, table=striked_out_table, properties=pg_properties)
    
    df = df.join(striked_df, on=(df["emp_id"] == striked_df["employee_id"]), how="left_anti")
    df = df.withColumn("start_date", to_date(from_unixtime(col("start_date").cast(DoubleType())))) \
           .withColumn("end_date", to_date(from_unixtime(col("end_date").cast(DoubleType()))))
    
    w1 = Window.partitionBy("emp_id", "start_date", "end_date").orderBy(col("salary").desc())
    df = df.withColumn("rn", row_number().over(w1)).filter(col("rn") == 1).drop("rn")

    w2 = Window.partitionBy("emp_id").orderBy("start_date")
    
    df = df.withColumn('next_start_date',lead("start_date").over(w2))\
            .withColumn("end_date",col("next_start_date"))\
                   .drop("next_start_date")
    
    df = df.withColumn("status", when(col("end_date").isNull(), lit("ACTIVE")).otherwise(lit("INACTIVE")))
    
    try:
        existing_df = spark.read.parquet(output_path).cache()
        existing_df.count()  # Force read
    except:
        existing_df = spark.createDataFrame([], df.schema)
    
    # Left-anti join to get only new records
    existing_keys = existing_df.select("emp_id", "start_date", "end_date", "salary").dropDuplicates()
    
    new_df = df.alias("new").join(
        existing_keys.alias("exist"),
        on=["emp_id", "start_date", "end_date", "salary"],
        how="left_anti"
    )
    
    combined = existing_df.unionByName(new_df)
    
    combined = combined.withColumn("next_start_date", lead("start_date").over(w2)) \
                       .withColumn("end_date", col("next_start_date")) \
                       .drop("next_start_date") \
                       .withColumn("status", when(col("end_date").isNull(), lit("ACTIVE")).otherwise(lit("INACTIVE")))
    
    w3 = Window.partitionBy("emp_id", "start_date", "end_date").orderBy(col("salary").desc())
    final_df = combined.withColumn("rn", row_number().over(w3)).filter(col("rn") == 1).drop("rn")
    
    # Write to Gold S3 (Parquet)
    final_df = final_df.join(striked_df.select("employee_id").distinct(), on=(striked_df["employee_id"] == final_df["emp_id"]), how="left") \
    .withColumn("status", when((col("status") == "ACTIVE") & (col("employee_id").isNotNull()), "INACTIVE")
                .otherwise(col("status"))) \
    .withColumn("end_date", when((col("status") == "INACTIVE") & (col("employee_id").isNotNull()), current_date())
                .otherwise(col("end_date")))
    final_df =final_df.drop("employee_id")
    
    
    final_df.write.mode("overwrite").partitionBy("status").parquet(output_path)
    
    print(" Written to Gold Layer.")
    
    # ==============================
    # PostgreSQL Connection & UPSERT
    # ==============================
    
    from pyspark.sql.types import LongType
    
    # PostgreSQL Connection Settings
    
    # Cast emp_id to match PostgreSQL bigint
    final_df = final_df.withColumn("emp_id", col("emp_id").cast(LongType()))
    
    # Write to PostgreSQL staging table
    final_df.select("emp_id", "designation", "start_date", "end_date", "salary", "status") \
        .write.mode("overwrite") \
        .jdbc(url=jdbc_url, table=pg_staging_table, properties=pg_properties)
    
    print(" Written to PostgreSQL staging table.")
    
    # Read staging and target tables
    staging_df = spark.read.jdbc(url=jdbc_url, table=pg_staging_table, properties=pg_properties)
    target_df = spark.read.jdbc(url=jdbc_url, table=pg_target_table, properties=pg_properties)
    
    # Perform UPSERT (merge logic)
    upsert_df = staging_df.alias("staging").join(
        target_df.alias("target"),
        on=["emp_id", "start_date", "end_date"],
        how="outer"
    ).select(
        when(col("staging.emp_id").isNotNull(), col("staging.emp_id")).otherwise(col("target.emp_id")).alias("emp_id"),
        when(col("staging.designation").isNotNull(), col("staging.designation")).otherwise(col("target.designation")).alias("designation"),
        when(col("staging.start_date").isNotNull(), col("staging.start_date")).otherwise(col("target.start_date")).alias("start_date"),
        when(col("staging.end_date").isNotNull(), col("staging.end_date")).otherwise(col("target.end_date")).alias("end_date"),
        when(col("staging.salary").isNotNull(), col("staging.salary")).otherwise(col("target.salary")).alias("salary"),
        when(col("staging.status").isNotNull(), col("staging.status")).otherwise(col("target.status")).alias("status")
    )
    
    # Overwrite the final target table
    upsert_df.write.mode("overwrite").jdbc(url=jdbc_url, table=pg_target_table, properties=pg_properties)
    
    print("PostgreSQL UPSERT completed.")
job.commit()    