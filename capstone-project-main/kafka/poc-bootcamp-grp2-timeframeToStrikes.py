import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *
# sys.path.insert(0, 's3://poc-bootcamp-capstone-project-group2/glue-libs/psycopg.zip')
import psycopg2
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#reading from the timeframe data
# schema = StructType([
#     StructField("emp_id", StringType(), True),
#     StructField("designation", StringType(), True),
#     StructField("start_date", LongType(), True),
#     StructField("end_date", DoubleType(), True),
#     StructField("salary", DoubleType(), True),
#     StructField("status",StringType(),True)
# ])
employee_timeframe_df = spark.read.format("parquet").load("s3://poc-bootcamp-capstone-project-group2/gold/employee-timeframe-opt/status=ACTIVE/*")
# filtering the employee ids from employee_timeframe data 
emp_ids = [row["emp_id"] for row in employee_timeframe_df.select("emp_id").collect()]
emp_id_list = ",".join(f"'{e}'" for e in emp_ids) 
emp_id_filter = f"({emp_id_list})"

print(emp_id_filter)
#query to read from the employee_strikes
query = f"(select * from employee_strikes where employee_id in {emp_id_filter}) as es"
            
#reading employee_strikes
employee_strikes_df = spark.read.format("jdbc")\
                            .option("dbtable", query) \
                            .option("user", "postgres") \
                            .option("password", "8308") \
                            .option("driver", "org.postgresql.Driver") \
                            .option("url", "jdbc:postgresql://54.165.21.137:5432/capstone_project2?socketTimeout=60&connectTimeout=30")\
                            .load() 
#filtering new records not in employee_strikes
new_records = employee_timeframe_df.join(employee_strikes_df,employee_timeframe_df["emp_id"]==employee_strikes_df["employee_id"],"left_anti").select("emp_id","salary")

#existing records with upated salary
existing_records = employee_timeframe_df.join(employee_strikes_df,(employee_timeframe_df["emp_id"]==employee_strikes_df["employee_id"]) &(employee_timeframe_df["salary"] != employee_strikes_df["salary"]),"inner")\
    .select(employee_timeframe_df["emp_id"],employee_timeframe_df["salary"],
           "strike_1","strike_2","strike_3","strike_4","strike_5","strike_6","strike_7","strike_8","strike_9","strike_10","no_of_strikes")

#strike calculator function
def func_strike_calculator(employee_strikes_df ):
    
    for i in range (1,11):
        salary_after_i_strikes = round(col("salary")*(0.9**i),2)
        employee_strikes_df = employee_strikes_df.withColumn(
            f"strike_{i}",
            when(
                (col("no_of_strikes")>=i),
                salary_after_i_strikes
            ).otherwise(lit(None))
        )

    return employee_strikes_df

#new_records_extended schema update
new_records_extended = new_records \
    .withColumn("strike_1", lit(None).cast("float")) \
    .withColumn("strike_2", lit(None).cast("float")) \
    .withColumn("strike_3", lit(None).cast("float")) \
    .withColumn("strike_4", lit(None).cast("float")) \
    .withColumn("strike_5", lit(None).cast("float")) \
    .withColumn("strike_6", lit(None).cast("float")) \
    .withColumn("strike_7", lit(None).cast("float")) \
    .withColumn("strike_8", lit(None).cast("float")) \
    .withColumn("strike_9", lit(None).cast("float")) \
    .withColumn("strike_10", lit(None).cast("float")) \
    .withColumn("no_of_strikes", lit(None).cast("int"))
#recalculate strikes on the updated salary
existing_records = func_strike_calculator(existing_records)

#union the salaries to be updated and to be appended
updated_df = existing_records.unionByName(new_records_extended).withColumnRenamed("emp_id","employee_id")

updated_df.write.format("jdbc")\
    .mode("overwrite")\
    .option("url","jdbc:postgresql://54.165.21.137:5432/capstone_project2")\
    .option("dbtable", "timeframeToStrikes_table") \
    .option("user", "postgres") \
    .option("password", "8308") \
    .option("driver", "org.postgresql.Driver") \
    .save() \

# Connect to PostgreSQL database
conn = psycopg2.connect(
    dbname="capstone_project2",
    user="postgres",
    password="8308",
    host="54.165.21.137",
    port="5432"
)

# Create a cursor object
cur = conn.cursor()

# Define the query
postgres_query = """
INSERT INTO employee_strikes (
                    employee_id, salary,
                    strike_1, strike_2, strike_3, strike_4, strike_5,
                    strike_6, strike_7, strike_8, strike_9, strike_10,
                    no_of_strikes
                )
                SELECT 
                    employee_id, salary,
                    strike_1, strike_2, strike_3, strike_4, strike_5,
                    strike_6, strike_7, strike_8, strike_9, strike_10,
                    no_of_strikes
                FROM timeframeToStrikes_table
                ON CONFLICT (employee_id) DO UPDATE SET
                    salary = EXCLUDED.salary,
                    strike_1 = EXCLUDED.strike_1,
                    strike_2 = EXCLUDED.strike_2,
                    strike_3 = EXCLUDED.strike_3,
                    strike_4 = EXCLUDED.strike_4,
                    strike_5 = EXCLUDED.strike_5,
                    strike_6 = EXCLUDED.strike_6,
                    strike_7 = EXCLUDED.strike_7,
                    strike_8 = EXCLUDED.strike_8,
                    strike_9 = EXCLUDED.strike_9,
                    strike_10 = EXCLUDED.strike_10,
                    no_of_strikes = EXCLUDED.no_of_strikes;
"""

# Execute the query
cur.execute(postgres_query)
        
# Commit the changes
conn.commit()

# Close cursor and connection
cur.close()
conn.close()








job.commit()