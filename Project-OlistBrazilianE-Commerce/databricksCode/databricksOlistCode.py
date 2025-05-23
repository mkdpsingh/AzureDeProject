# Databricks notebook source
import threading
from datetime import datetime, timedelta

def run_spark():
    spark
    ist_time = datetime.utcnow() + timedelta(hours=5, minutes=30)
    print(f"Executed at: {ist_time}")
    threading.Timer(1140, run_spark).start()

run_spark()

# COMMAND ----------

storage_account = "mksoliststorageaccount"
application_id = "5e4dc1f8-f429-4eb6-8888-4f4abd15d59f"
directory_id = "15e203e9-bdac-4076-bc61-29fb7005e633"
service_credential = "olistAppRegistration - Secrets and certificates - value"

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", 
               f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

df = spark.read.format("csv").option("header", "true").load(f"abfss://olistdata@mksoliststorageaccount.dfs.core.windows.net/bronze/olist_customers_dataset.csv")
# display(df)

# COMMAND ----------

base_path = "abfss://olistdata@mksoliststorageaccount.dfs.core.windows.net/bronze/"
orders_path = base_path + "olist_orders_dataset.csv"
payments_path = base_path + "olist_order_payments_dataset.csv"
reviews_path = base_path + "olist_order_reviews_dataset.csv"
items_path = base_path + "olist_order_items_dataset.csv"
customers_path = base_path + "olist_customers_dataset.csv"
sellers_path = base_path + "olist_sellers_dataset.csv"
geolocation_path = base_path + "olist_geolocation_dataset.csv"
products_path = base_path + "olist_products_dataset.csv"

orders_df = spark.read.format("csv").option("header", "true").load(orders_path)
payments_df = spark.read.format("csv").option("header", "true").load(payments_path)
reviews_df = spark.read.format("csv").option("header", "true").load(reviews_path)
items_df = spark.read.format("csv").option("header", "true").load(items_path)
customers_df = spark.read.format("csv").option("header", "true").load(customers_path)
sellers_df = spark.read.format("csv").option("header", "true").load(sellers_path)
geolocation_df = spark.read.format("csv").option("header", "true").load(geolocation_path)
products_df = spark.read.format("csv").option("header", "true").load(products_path)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Data From Mongo DB

# COMMAND ----------

import pymongo
from pymongo import MongoClient

# COMMAND ----------

# importing module
from pymongo import MongoClient

hostname = "pjdir.h.filess.io"
database = "olistProjectNoSQL_sailpainus"
port = "27018"
username = "olistProjectNoSQL_sailpainus"
password = "9a884b18a31f961088eedffdc4063ba8370c160f"

uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database

# Connect with the portnumber and host
client = MongoClient(uri)

# Access database
mydatabase = client[database]
mydatabase

# COMMAND ----------

import pandas as pd
collection = mydatabase['product_categories']
display(collection)
mongo_data = pd.DataFrame(collection.find())
# mongo_data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning the Data

# COMMAND ----------

from pyspark.sql.functions import col,datediff,to_date,current_date,when


# COMMAND ----------

def clean_df(df):
    return df.dropDuplicates().na.drop("all")

initial_count = orders_df.count()
orders_df = clean_df(orders_df)
final_count = orders_df.count()

print(f"Initial count: {initial_count}, Final count: {final_count}")
# display(orders_df)

# COMMAND ----------

# convert Date Colums

orders_df = orders_df.withColumn("order_purchase_timestamp", to_date(col("order_purchase_timestamp")))\
    .withColumn("order_delivered_customer_date", to_date(col("order_delivered_customer_date")))\
        .withColumn("order_estimated_delivery_date", to_date(col("order_estimated_delivery_date")))

# COMMAND ----------

# Calculate Delivery and Time Delays

orders_df = orders_df.withColumn("actual_delivery_time", datediff("order_delivered_customer_date", "order_purchase_timestamp"))
orders_df = orders_df.withColumn("estimated_delivery_time", datediff("order_estimated_delivery_date", "order_purchase_timestamp"))
orders_df =orders_df.withColumn("Delay Time", col("actual_delivery_time") - col("estimated_delivery_time"))

# display(orders_df)

# COMMAND ----------

# display(orders_df.tail(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joining

# COMMAND ----------

payments_df.columns,items_df.columns

# COMMAND ----------

orders_cutomers_df = orders_df.join(customers_df, orders_df.customer_id == customers_df.customer_id,"left")

orders_payments_df = orders_cutomers_df.join(payments_df, orders_cutomers_df.order_id == payments_df.order_id,"left")

orders_items_df = orders_payments_df.join(items_df,"order_id","left")

orders_items_products_df = orders_items_df.join(products_df, orders_items_df.product_id == products_df.product_id,"left")

final_df = orders_items_products_df.join(sellers_df, orders_items_products_df.seller_id == sellers_df.seller_id,"left")

# COMMAND ----------

# display(final_df)

# COMMAND ----------

mongo_data.drop('_id',axis=1,inplace=True)

mongo_sparf_df = spark.createDataFrame(mongo_data)
# display(mongo_sparf_df)

# COMMAND ----------

final_df = final_df.join(mongo_sparf_df,"product_category_name","left")

# COMMAND ----------

# display(final_df)

# COMMAND ----------

def remove_duplicate_columns(df):
    columns = df.columns

    seen_columns = set()
    columns_to_drop = []

    for column in columns:
        if column in seen_columns:
            columns_to_drop.append(column)
        else:
            seen_columns.add(column)
    
    df_cleaned = df.drop(*columns_to_drop)
    return df_cleaned

final_df = remove_duplicate_columns(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").parquet("abfss://olistdata@mksoliststorageaccount.dfs.core.windows.net/silver")

# COMMAND ----------

display(final_df)

# COMMAND ----------

