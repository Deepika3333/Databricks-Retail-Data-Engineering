# Databricks notebook source
# Bronze Layer – Data Ingestion

# If using Databricks Repo:
# %run ./config/project_config
from pyspark.sql import functions as F
# Config
source_table = "retail.landing_zone.retail_data"
target_table = "retail.bronze.retail_transactions"
spark.sql("CREATE SCHEMA IF NOT EXISTS retail.bronze")
# -----------------------------
# DROP existing Bronze table
# (avoids DELTA_FAILED_TO_MERGE_FIELDS)
spark.sql(f"DROP TABLE IF EXISTS {target_table}")
# -----------------------------
# Read from landing_zone
# -----------------------------
df_raw = spark.table(source_table)
# Drop unnamed columns if present
df_raw = df_raw.drop(*[c for c in df_raw.columns if c.startswith("_c")])
# ----------------------------
# Cast + select (NO columns removed)
# -----------------------------
df_bronze = df_raw.select(
F.col("Transaction_ID").cast("int").alias("Transaction_ID"),
F.col("Customer_ID").cast("int").alias("Customer_ID"),
F.col("Name").cast("string").alias("Name"),
F.col("Email").cast("string").alias("Email"),
F.col("Phone").cast("string").alias("Phone"),
F.col("Address").cast("string").alias("Address"),
F.col("City").cast("string").alias("City"),
F.col("State").cast("string").alias("State"),
F.col("Zipcode").cast("string").alias("Zipcode"),
F.col("Country").cast("string").alias("Country"),
F.col("Age").cast("int").alias("Age"),
F.col("Gender").cast("string").alias("Gender"),
F.col("Income").cast("string").alias("Income"),
F.col("Customer_Segment").cast("string").alias("Customer_Segment"),
# Keep raw in Bronze; parse in Silver
F.col("Date").cast("string").alias("Date"),
F.col("Year").cast("int").alias("Year"),
F.col("Month").cast("string").alias("Month"),
F.col("Time").cast("string").alias("Time"),
F.col("Total_Purchases").cast("int").alias("Total_Purchases"),
F.col("Amount").cast("double").alias("Amount"),
F.col("Total_Amount").cast("double").alias("Total_Amount"),
F.col("Product_Category").cast("string").alias("Product_Category"),
F.col("Product_Brand").cast("string").alias("Product_Brand"),
F.col("Product_Type").cast("string").alias("Product_Type"),
F.col("Feedback").cast("string").alias("Feedback"),
F.col("Shipping_Method").cast("string").alias("Shipping_Method"),
F.col("Payment_Method").cast("string").alias("Payment_Method"),
F.col("Order_Status").cast("string").alias("Order_Status"),
F.col("Ratings").cast("int").alias("Ratings"),
F.col("products").cast("string").alias("products")
)
# Write Bronze table
# -----------------------------
df_bronze.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(target_table)
