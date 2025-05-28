import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, year, month
from awsglue.job import Job


# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Fetch resolved arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize the Job with the correct parameters
job = Job(glueContext)
job.init(args['JOB_NAME'], args)  # Pass args here correctly

# Define the source and destination paths
bronze_bucket = "s3://poc-bootcamp-capstone-project-group2/bronze/employee_calendar/"
silver_bucket = "s3://poc-bootcamp-capstone-project-group2/silver/employee_leave_calendar/"

# PostgreSQL connection details
pg_url = "jdbc:postgresql://54.165.21.137:5432/capstone_project2"
pg_properties = {
    "user": "postgres",
    "password": "8308",
    "driver": "org.postgresql.Driver"
}
pg_table = "employee_leave_calendar_final_table" 

# Define the schema explicitly
schema = StructType([
    StructField("reason", StringType(), True),
    StructField("date", TimestampType(), True)
])

# Read the data from the bronze bucket
employee_leave_df = spark.read.format("csv").schema(schema).load(bronze_bucket)
if employee_leave_df.rdd.isEmpty():
    print("No data found in input. Skipping processing and write steps.")
else :
        
    # Drop duplicate records
    employee_leave_df = employee_leave_df.dropDuplicates()
    
    # Add year column
    transformed_df = employee_leave_df.withColumn("year", year(col("date")))
    
    # Write to silver bucket in Parquet format, partitioned by year
    transformed_df.write.mode("append").partitionBy("year").parquet(silver_bucket)
    
    transformed_df.write \
            .jdbc(pg_url, table=pg_table, mode="append", properties=pg_properties)
    # Commit the job
job.commit()
