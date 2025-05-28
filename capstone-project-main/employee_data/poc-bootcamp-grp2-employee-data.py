import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket = "poc-bootcamp-capstone-project-group2"
bronze_prefix = "bronze/employee-data-opt/"
gold_prefix = "gold/employee-data-opt-output/"

input_path = f"s3://{bucket}/{bronze_prefix}"
output_path = f"s3://{bucket}/{gold_prefix}"

# Partition values
today = datetime.utcnow()
year = today.strftime('%Y')
month = today.strftime('%m')
day = today.strftime('%d')

# PostgreSQL connection details
pg_url = "jdbc:postgresql://54.165.21.137:5432/capstone_project2"
pg_properties = {
    "user": "postgres",
    "password": "8308",
    "driver": "org.postgresql.Driver"
}
pg_table = "employee_data_final_table"  

# Read data
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("emp_id", LongType(), True)
])

df = spark.read.schema(schema).option("header", "true").csv(input_path)

if not df.rdd.isEmpty():
    df_clean = (
        df.dropna(subset=["emp_id", "name", "age"])
          .filter(col("age") > 0)
          .dropDuplicates(["emp_id", "name", "age"])
          .withColumn("year", lit(year))
          .withColumn("month", lit(month))
          .withColumn("day", lit(day))
    )

    # Write to S3 (Gold)
    df_clean.write.mode("append").partitionBy("year", "month", "day").parquet(output_path)

    # Write to PostgreSQL
    df_clean.select("emp_id", "name", "age").write \
        .jdbc(pg_url, table=pg_table, mode="append", properties=pg_properties)

job.commit()
