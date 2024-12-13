# Databricks notebook source
#Neccessary libraries for the program 
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import size,col, collect_set, array_contains


#To create a Spark environment 
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("PySpark Assignment").getOrCreate() 


##---------Question 1-------------
# Define schema for the dataframe  that needs to be created 
# purchase data dataframe 
purchase_schema = StructType([
    StructField("customer", IntegerType(), True),
    StructField("product_model", StringType(), True)
])

# Data from the  question 
purchase_data = [
    (1, "iphone13"), (1, "dell i5 core"), (2, "iphone13"), 
    (2, "dell i5 core"), (3, "iphone13"), (3, "dell i5 core"), 
    (1, "dell i3 core"), (1, "hp i5 core"), (1, "iphone14"), 
    (3, "iphone14"), (4, "iphone13")
]

# Create DataFrame syntax for purchase data 
purchase_data_df = spark.createDataFrame(purchase_data, schema=purchase_schema)

#To see the dataframe 
purchase_data_df.show()

# Define schema for product data dataframe 
product_schema = StructType([
    StructField("product_model", StringType(), True)
])

# Data from the question 
product_data = [
    ("iphone13",), ("dell i5 core",), ("dell i3 core",), 
    ("hp i5 core",), ("iphone14",)
]

# Create DataFrame for product data 
product_data_df = spark.createDataFrame(product_data, schema=product_schema)

#To see the dataframe 
product_data_df.show()


#The customers who have bought only iphone13

only_iphone13_df = purchase_data_df.groupBy("customer") \
    .agg(collect_set("product_model").alias("products")) \
    .filter((array_contains(col("products"), "iphone13")) & (size(col("products")) == 1))

only_iphone13_df.show()


#The customers who upgraded from product iphone13 to product iphone14

upgrade_df = purchase_data_df.groupBy("customer") \
    .agg(collect_set("product_model").alias("products")) \
    .filter(array_contains(col("products"), "iphone13") & array_contains(col("products"), "iphone14"))

upgrade_df.show()



#Customers who have bought all models in the new Product Data

all_models = product_data_df.select("product_model").rdd.flatMap(lambda x: x).collect()

all_models_df = purchase_data_df.groupBy("customer") \
    .agg(collect_set("product_model").alias("products")) \
    .filter(size(col("products")) == len(all_models))

all_models_df.show()
















