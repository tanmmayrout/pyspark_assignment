# Databricks notebook source

#Neccessary libraries for the program
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, date_sub, col

##---------Question 3-------------
# Define schema for the dataframe  that needs to be created 
# User acticity data dataframe
# Define schema with TimestampType for 'time_stamp' 
schema = StructType([
    StructField("log_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("user_activity", StringType(), True),
    StructField("time_stamp", StringType(), True)  # Temporarily keep this as type string, convert to timestamp later 
])

# Data from the question 
data = [
    (1, 101, 'login', '2023-09-05 08:30:00'),
    (2, 102, 'click', '2023-09-06 12:45:00'),
    (3, 101, 'click', '2023-09-07 14:15:00'),
    (4, 103, 'login', '2023-09-08 09:00:00'),
    (5, 102, 'logout', '2023-09-09 17:30:00'),
    (6, 101, 'click', '2023-09-10 11:20:00'),
    (7, 103, 'click', '2023-09-11 10:15:00'),
    (8, 102, 'click', '2023-09-12 13:10:00')
]

# DataFrame with the schema for user activity 
user_activity_df = spark.createDataFrame(data, schema)

# Convert 'time_stamp' column to TimestampType
user_activity_df = user_activity_df.withColumn("time_stamp", F.to_timestamp("time_stamp", "yyyy-MM-dd HH:mm:ss"))

# Show the dataFrame with the timestamp which we converted 
user_activity_df.show()


# Function definition to rename columns dynamically 
def rename_columns(df, new_columns):
    old_columns = df.columns
    for old_col, new_col in zip(old_columns, new_columns):
        df = df.withColumnRenamed(old_col, new_col)
    return df

# new column name definitions 
new_columns = ['log_id', 'user_id', 'user_activity', 'time_stamp']

# Dynamic rename 
user_activity_df = rename_columns(user_activity_df, new_columns)

user_activity_df.show()


# filter data for the last 7 days
last_7_days_df = user_activity_df.filter(col("time_stamp") >= date_sub(current_date(), 7))

# Group by user_id and online actions (count the number of actions) 
action_count_df = last_7_days_df.groupBy("user_id").count().alias("action_count")

action_count_df.show()


# Write DataFrame as CSV with different write options
user_activity_df.write \
    .option("header", "true") \
    .option("delimiter", ",") \
    .mode("overwrite") \
    .csv("path/to/output/csv_file")

# Create or use the `user` database
spark.sql("CREATE DATABASE IF NOT EXISTS user")

# write the dataFrame as a managed table with overwrite mode
user_activity_df.write \
    .mode("overwrite") \
    .saveAsTable("user.login_details")

print("Table user.login_details has been written successfully.")




