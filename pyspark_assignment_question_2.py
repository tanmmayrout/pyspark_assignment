# Databricks notebook source

#Neccessary libraries for the program
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

##---------Question 2-------------
# Define schema for the dataframe  that needs to be created 
# credit card data dataframe 
# data from the question 
data = [("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)]

# Create DataFrame from credit card data 
credit_card_df = spark.createDataFrame(data, ["card_number"])

credit_card_df.show()

# print number of partitions
print("Number of partitions:", credit_card_df.rdd.getNumPartitions())

# Change the partition size to 5
credit_card_df = credit_card_df.repartition(5)
print("Changed number of partitions:", credit_card_df.rdd.getNumPartitions())

# Change the partition size back to its original partition size
credit_card_df = credit_card_df.repartition(8)  
print("Decreased number of partitions:", credit_card_df.rdd.getNumPartitions())

# UDF to print only the last 4 digits marking the remaining digits as *
# UDF function definition 
def mask_card_number(card_number):
    return '*' * (len(card_number) - 4) + card_number[-4:] # Returns * for the last 4 digits of the credit card number 

# Register UDF
mask_card_udf = udf(mask_card_number, StringType())

# Add new column to show the masked credit card numbers on the new dataframe 
masked_df = credit_card_df.withColumn("masked_card_number", mask_card_udf("card_number"))

masked_df.show()




