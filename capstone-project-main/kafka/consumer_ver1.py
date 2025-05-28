#import libraries
from pyspark.sql import SparkSession
import json, psycopg2, select ,signal , sys, re
from pyspark.sql.types import *
from pyspark.sql.functions import col , lit , count, when ,round ,coalesce, udf, md5, concat_ws, from_json, current_timestamp
from datetime import datetime, timedelta
from pyspark import StorageLevel

spark = SparkSession.builder.appName("kafka-consumer-full").config("spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0") \
        .getOrCreate()

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#Step A: build postgress query


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
                FROM employee_strikes_stg
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


#----------------------------------------------------------------------------------------------------------------------------------------------------------------------



# Step B: perform spark oriented operations

#reading from the kafka connection
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "employee-messages") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 10) \
    .load()

# --- Message Schema ---
message_schema = StructType() \
    .add("sender", StringType()) \
    .add("receiver", StringType()) \
    .add("message", StringType())

# --- Load flagged words ( from S3) and vocabs ---
# Load vocab and marked words
with open("s3://poc-bootcamp-capstone-project-group2/silver/marked_words/") as f:
    vocab = set(json.load(f))
with open("s3://poc-bootcamp-capstone-project-group2/silver/vocabs/") as f:
    marked_words = set(json.load(f))



#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#function to recompute the strikes based on the difference between the 'strike_count' and the 'no_of_strikes'
def func_strike_calculator(employee_strikes_df,employee_flags_count_df ):
    # Step 1: Join
    employee_combined_df = employee_strikes_df.join(
        employee_flags_count_df,
        employee_strikes_df["employee_id"]==employee_flags_count_df["employee_id"],
        how="inner"
    ).select(employee_strikes_df["employee_id"],"salary","strike_1","strike_2","strike_3","strike_4","strike_5","strike_6","strike_7","strike_8","strike_9","strike_10","no_of_strikes","strike_count")

    # Step 2: Process strike columns
    updated_df = employee_combined_df
    
    for i in range(1, 11):
        salary_after_i_strikes = round(col("salary") * (0.9 ** i),2)
        updated_df = updated_df.withColumn(
            f"strike_{i}",
            when(
                (col("strike_count") >= i) & (col("no_of_strikes") < i),
                salary_after_i_strikes
            ).when(
                (col("strike_count") < i),
                lit(None)
            ).otherwise(
                col(f"strike_{i}")
            )
        )
    # Step 3: Update no_of_strikes
    updated_df = updated_df.withColumn(
        "no_of_strikes",
        coalesce(col("strike_count"),lit(0))
    ).drop("strike_count")
    
    return updated_df

#--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# ----- Processing logic for each batch -----
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    
    # Connect to PostgreSQL database
    conn = psycopg2.connect(
        dbname="capstone_project2",
        user="postgres",
        password="8308",
        host="localhost",
        port="5432"
    )
    
    # Create a cursor object
    cur = conn.cursor()


    # Step 1: Parse Kafka message
    json_df = batch_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), message_schema).alias("data")) \
        .select("data.*")
    def is_flagged(message):
        words = re.findall(r'\b\w+\b', message.upper())  # Tokenize & normalize to uppercase
        return (
            any(word in marked_words for word in words) and
            all(word in vocab for word in words)
        )
    
    is_flagged_udf = udf(is_flagged, BooleanType())

    flagged_df = json_df \
        .withColumn("is_flagged", is_flagged_udf(col("message"))) \
        .filter(col("is_flagged")) \
        .withColumn("start_date", current_timestamp()) \
        .select(col("sender").alias("employee_id"), "start_date")
    
    # Step 2: Filter flagged messages within last month
    one_month_ago = datetime.utcnow() - timedelta(days=30)
    one_month_ago_str = one_month_ago.strftime('%Y-%m-%d %H:%M:%S')
    flagged_df = flagged_df.filter(col("start_date") >= lit(one_month_ago_str))
    
    #Step 3: Get unique employee IDs in current batch
    emp_ids = [row["employee_id"] for row in flagged_df.select("employee_id").distinct().collect()]
    if not emp_ids:
        return
        
    #Step 4.1: Construct IN clause safely (Postgres can handle ~10,000 values)
    emp_id_list = ",".join(f"'{e}'" for e in emp_ids)  # quote if TEXT type
    emp_id_filter = f"({emp_id_list})"

    flagged_query = f"""
        (SELECT employee_id, start_date
         FROM flagged_messages
         WHERE employee_id IN {emp_id_filter}
           AND start_date >= CURRENT_DATE - INTERVAL '30 days') AS fm
    """

    #Step 4.2: reading only relevant records from the postgres table
    flagged_history_df = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/capstone_project2") \
        .option("dbtable", flagged_query) \
        .option("user", "postgres") \
        .option("password", "8308") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    #Step 5:combine the latest flags with historical flags
    combined_df = flagged_history_df.unionByName(flagged_df) \
        .filter(col("start_date") >= lit(one_month_ago_str))

    #Step 6:count the valid flags for today
    flag_count_df = combined_df.groupBy("employee_id").count().withColumnRenamed("count", "strike_count")

    # Step 7.1:--- Read only relevant strikes
    strikes_query = f"""
        (SELECT * FROM employee_strikes
         WHERE employee_id IN {emp_id_filter}) AS es
    """

    #Step 7.2: reading only the relevant strikes
    employee_strikes_df = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/capstone_project2") \
        .option("dbtable", strikes_query) \
        .option("user", "postgres") \
        .option("password", "8308") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    #Step 7.3: remove the records with strikes 10
    employee_to_exclude = employee_strikes_df.filter(col("no_of_strikes")==10)
    
    #Step 7.3.1: Get the list of employee_ids with 10 strikes
    excluded_ids = [row["employee_id"] for row in employee_to_exclude.select("employee_id").distinct().collect()]
    
    # Step 7.3.2: Filter out those employee_ids from flags_count_df
    flag_count_df = flag_count_df.filter(~col("employee_id").isin(excluded_ids))
     
    
    # Step 8: Compute updated employee strikes
    new_employee_strikes_df = func_strike_calculator(employee_strikes_df, flag_count_df)

    #Step 9: Find only changed rows  Add hash to detect changes
    old_with_hash = employee_strikes_df.withColumn("hash", md5(concat_ws("||", *employee_strikes_df.columns)))
    new_with_hash = new_employee_strikes_df.withColumn("hash", md5(concat_ws("||", *new_employee_strikes_df.columns)))

    # Step 10:Join on primary key (assume employee_id) and filter where hash is different
    updated_df = new_with_hash.alias("new").join(
        old_with_hash.alias("old"),
        on="employee_id",
        how="inner"
    ).filter(col("new.hash") != col("old.hash")) \
     .select("new.*")
    
    # Step 11: write updated rows to a staging table
    updated_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/capstone_project2") \
        .option("dbtable", "employee_strikes_stg") \
        .option("user", "postgres") \
        .option("password", "8308") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    
    # Step 12: Also append flagged_df to append-only table
    flagged_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/capstone_project2") \
        .option("dbtable", "flagged_messages") \
        .option("user", "postgres") \
        .option("password", "8308") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    #for striked_out empoyees
    striked_out_employees = updated_df.where(col("no_of_strikes")==10).select("employee_id")
    if striked_out_employees is not None:
        striked_out_employees.write.format("parquet").mode("append").save("s3://poc-bootcamp-capstone-project-group2/silver/striked_out/")
        striked_out_employees.write\
            .format("jdbc") \
            .option("url","jdbc:postgresql://localhost:5432/capstone_project2") \
            .option("dbtable", "striked_out") \
            .option("user", "postgres") \
            .option("password", "8308") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        striked_out_ids = [row["employee_id"] for row in striked_out_employees.select("employee_id").collect()]
        cur.execute("""
        UPDATE employee_timeframe_final_table
        SET status = 'INACTIVE'
        WHERE employee_id = ANY(%s)
    """, (striked_out_ids,))

        
    # #if updated_df  not null
    # if updated is not None:
    #     listen_for_changes()
    if updated_df is not None:
        # Execute the query
        cur.execute(postgres_query)
                
        # Commit the changes
        conn.commit()
        
        # Close cursor and connection
        cur.close()
        conn.close()
        
#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------


# Step 13: ---Stream Trigger to start the processing of the stream ----
query = kafka_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .option("checkpointLocation", "s3://poc-bootcamp-capstone-project-group2/silver/kafka-checkpoint/") \
    .trigger(processingTime = "30 seconds")\
    .start()

query.awaitTermination()























