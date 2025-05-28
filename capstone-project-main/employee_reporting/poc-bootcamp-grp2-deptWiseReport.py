import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from datetime import datetime
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# ====================== Part 1: Active Employees by Designation ======================
time_df = spark.read.parquet('s3://poc-bootcamp-capstone-project-group2/gold/employee-timeframe-opt/')

active_df = spark.read.parquet('s3://poc-bootcamp-capstone-project-group2/gold/employee-timeframe-opt/status=ACTIVE/')

active_count_df = active_df.groupBy("designation").agg(count(col("emp_id")).alias("active_employees"))
designation_df = time_df.select("designation").distinct()

final_cnt_df = designation_df.join(active_count_df, on="designation", how="left").fillna({"active_employees": 0})

today_str = datetime.utcnow().strftime("%Y-%m-%d")
final_cnt_df.withColumn("run_date", lit(today_str)) \
    .write.mode('overwrite').partitionBy("run_date") \
    .parquet('s3://poc-bootcamp-capstone-project-group2/gold/active_emp_by_desg/')

# PostgreSQL Connection Settings
pg_host = "54.165.21.137"
pg_port = "5432"
pg_user = "postgres"
pg_password = "8308"
pg_dbname = "capstone_project2"
pg_staging_table = "active_emp_by_desg_staging"
pg_target_table = "active_employee_by_designation_final_table"

# JDBC URL for PostgreSQL
jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_dbname}"

# JDBC properties
pg_properties = {
    "user": pg_user,
    "password": pg_password,
    "driver": "org.postgresql.Driver"
}

final_cnt_df.write \
    .mode("overwrite") \
    .jdbc(url=jdbc_url, table=pg_target_table, properties=pg_properties)

print("Written df to target table.")



# Commit job
job.commit()
