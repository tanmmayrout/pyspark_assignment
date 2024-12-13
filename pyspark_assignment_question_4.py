# Databricks notebook source
#Neccessary libraries for the program
from pyspark.sql.functions import col, current_date, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import explode, explode_outer, posexplode
import re 

##---------Question 3-------------

# Reading JSON file with dynamic schema
json_df = spark.read.option("multiline", "true").json("/FileStore/tables/nested_json_file.json")

# Show the schema and data
json_df.printSchema()
json_df.show(truncate=False)


# Function to flatten the DataFrame
def flatten(df):
    # Iterate through each column and expand nested structs or arrays
    flat_cols = []
    for col_name in df.columns:
        if isinstance(df.schema[col_name].dataType, StructType):
            for field in df.schema[col_name].dataType.fields:
                flat_cols.append(col(col_name + "." + field.name).alias(col_name + "_" + field.name))
        else:
            flat_cols.append(col(col_name))
    return df.select(flat_cols)

# Flattened df 
flattened_df = flatten(json_df)
flattened_df.show(truncate=False)

# Count records before and after flattening
original_count = json_df.count()
flattened_count = flattened_df.count()

# the record count when flattened and when it's not flattened
print(f"Original record count: {original_count}")
print(f"Flattened record count: {flattened_count}")

# the difference using explode, explode outer, posexplode functions
# Example with explode
df_exploded = json_df.select(explode(col("employees")).alias("exploded_data"))
df_exploded.show()

# Example with explode_outer (same as explode but preserves nulls for empty arrays)
df_exploded_outer = json_df.select(explode_outer(col("employees")).alias("exploded_data_outer"))
df_exploded_outer.show()

# Example with posexplode (same as explode, but includes the position of the array elements)
df_exploded_pos = json_df.select(posexplode(col("employees")).alias("pos", "exploded_data"))
df_exploded_pos.show()

# Filter the id which is equal to 0001 
filtered_df = flattened_df.filter(col("id") == "1001") # changed to 1001 because 0001 was not present
filtered_df.show(truncate=False)

# convert the column names from camel case to snake case

# Function to convert camelCase to snake_case
def camel_to_snake(name):
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower() # logic from the internet 

# Rename columns in the DataFrame
for column in flattened_df.columns:
    flattened_df = flattened_df.withColumnRenamed(column, camel_to_snake(column))

flattened_df.show(truncate=False)


# Add the load_date column with the current date
flattened_df = flattened_df.withColumn("load_date", current_date()) #used current date 
flattened_df.show(truncate=False)


# Extract year, month, and day from the load_date
flattened_df = flattened_df.withColumn("year", year("load_date")) \
                           .withColumn("month", month("load_date")) \
                           .withColumn("day", dayofmonth("load_date"))

flattened_df.show(truncate=False)


# Check the current database/schema
spark.catalog.currentDatabase()

# Write DataFrame to a managed table with partitioning to default database on databricks 
flattened_df.write \
    .partitionBy("year", "month", "day") \
    .format("json") \
    .mode("overwrite") \
    .saveAsTable("default.employee_details")

table_df = spark.table("default.employee_details")
table_df.show(truncate=False)








