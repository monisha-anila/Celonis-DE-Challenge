import boto3
import sys
import json
import time
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

print("read glue data")
datasource = glueContext.create_dynamic_frame.from_catalog(
    database="online-retail-glue-db",
    table_name="csvfiles",
    push_down_predicate="timeframe >= '2025-04-19'"
)

if datasource.count() == 0:
    print("No data found for the specified partition.")
    job.commit()
else:
    # Proceed with processing
    df = datasource.toDF()
    print("df is created!")
    # df.describe().show()
    # df.printSchema()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    from pyspark.sql.functions import col, sum, count, to_date, to_timestamp, to_utc_timestamp

    readj_df = df.withColumn("InvoiceDate",to_timestamp(col("InvoiceDate"),"dd/MM/yy H:mm"))
    
    print("Invoice date is converted to timestamp format")

    removed_dup_df = readj_df.dropDuplicates(subset=['InvoiceNo','StockCode','Description','Quantity','InvoiceDate',
    'UnitPrice','CustomerID','Country'])

    print("Duplicates is removed.")
# readj_df.groupBy(readj_df.columns) \
#   .count() \
#   .filter("count > 1") \
#   .show(truncate=False)


# for col_name in removed_dup_df.columns:
#     null_count = removed_dup_df.filter(f"{col_name} IS NULL").count()
#     print(f"{col_name}: {null_count} nulls")

    final_df = removed_dup_df.na.drop(how = 'any',subset=['InvoiceNo','StockCode','Description','Quantity','InvoiceDate',
    'UnitPrice','CustomerID','Country'])
    
    print("Null values is removed.")

    # final_df.count()

## Select the column
## Filter the null data
## Replace the null data with no value

## final_df.select('CustomerID').filter("CustomerID IS NULL").na.fill("No value",subset='CustomerID').show()

# # Now save the DataFrame as a new CSV
# final_df.write.option("header", "true").csv("/FileStore/tables/output.csv")


    print("start reading db details")
# Extract DB credentials
    username = 'username'
    password = 'password'
    host = 'host'
    port = 5432
    database = 'database'

    print("get jdbc url")
    jdbc_url = "jdbc:postgresql://host:5432/database"

# Your DataFrame (replace this with your actual input)
# df = glueContext.create_dynamic_frame.from_catalog(...).toDF()

    print("starting to write to db")
# Write to PostgreSQL (first time: append)
    final_df.write \
     .format("jdbc") \
     .option("url", jdbc_url) \
     .option("dbtable", "online_retail_table") \
     .option("user", username) \
     .option("password", password) \
     .option("driver", "org.postgresql.Driver") \
     .mode("append") \
     .save()


    job.commit()