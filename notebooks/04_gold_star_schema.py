# ============================================================
# Layer: Gold
# Purpose:
#   Build analytics-ready star schema for Power BI reporting.
#   Create dimension and fact tables using deterministic
#   business keys (no surrogate IDs).
#
# Data Model:
#   Fact Table:
#     - fact_sales
#
#   Dimension Tables:
#     - dim_date
#     - dim_country
#     - dim_product
#     - dim_customer_profile
#     - dim_shipping
#     - dim_order_status
#     - dim_feedback
#
# Input Table:
#   retail.silver.retail_transactions_clean
#
# Output Schema:
#   retail.gold
#
# Author: Deepika Mandapalli
# ============================================================

from pyspark.sql import functions as F
# Purpose:
#   Build analytics-ready star schema for Power BI.
#   Use deterministic Business Keys (stable joins).
# ============================================================

# -----------------------------
# Config
# -----------------------------
silver_table = "retail.silver.retail_transactions_clean"
gold_schema = "retail.gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_schema}")

# -----------------------------
# Read Silver
# -----------------------------
df = spark.table(silver_table)

# ============================================================
# 0) Create deterministic Business Keys (NOT surrogate keys)
# ============================================================

# Transaction_BK = Transaction_ID|Country
if "Transaction_BK" not in df.columns and "Transaction_ID" in df.columns and "Country" in df.columns:
    df = df.withColumn(
        "Transaction_BK",
        F.concat_ws("|", F.col("Transaction_ID").cast("string"), F.col("Country"))
    )

# Product_BK = Category|Brand|Type|Product_Name
df = df.withColumn(
    "Product_BK",
    F.concat_ws(
        "|",
        F.col("Product_Category"),
        F.col("Product_Brand"),
        F.coalesce(F.col("Product_Type"), F.lit("UNKNOWN")),
        F.col("Product_Name")
    )
)

# CustomerProfile_BK = Gender|Income|Customer_Segment
df = df.withColumn(
    "CustomerProfile_BK",
    F.concat_ws("|", F.col("Gender"), F.col("Income"), F.col("Customer_Segment"))
)

# Feedback_BK = normalized Feedback
df = df.withColumn("Feedback_BK", F.upper(F.trim(F.col("Feedback"))))

# ============================================================
# 1) DROP Gold tables first (idempotent runs)
# ============================================================
drop_tables = [
    f"{gold_schema}.dim_date",
    f"{gold_schema}.dim_country",
    f"{gold_schema}.dim_product",
    f"{gold_schema}.dim_customer_profile",
    f"{gold_schema}.dim_shipping",
    f"{gold_schema}.dim_order_status",
    f"{gold_schema}.dim_feedback",
    f"{gold_schema}.fact_sales"
]

for t in drop_tables:
    spark.sql(f"DROP TABLE IF EXISTS {t}")

# ============================================================
# 2) DIMENSIONS (each key UNIQUE => "1" side)
# ============================================================

# dim_date (key = transaction_date)
dim_date = (
    df.select("transaction_date")
      .dropDuplicates()
      .withColumn("year", F.year("transaction_date"))
      .withColumn("month_num", F.month("transaction_date"))
      .withColumn("month_name", F.date_format("transaction_date", "MMM"))
      .withColumn("year_month", F.date_format("transaction_date", "yyyy-MM"))
      .withColumn("quarter", F.concat(F.lit("Q"), F.quarter("transaction_date")))
)

# dim_country (key = Country)
dim_country = df.select("Country").dropDuplicates()

# dim_shipping (key = Shipping_Method)
dim_shipping = df.select("Shipping_Method").dropDuplicates()

# dim_order_status (key = Order_Status)
dim_order_status = df.select("Order_Status").dropDuplicates()

# dim_feedback (key = Feedback_BK -> renamed to Feedback)
dim_feedback = (
    df.select("Feedback_BK")
      .dropDuplicates()
      .withColumnRenamed("Feedback_BK", "Feedback")
)

# dim_product (key = Product_BK)
dim_product = (
    df.select("Product_BK", "Product_Category", "Product_Brand", "Product_Type", "Product_Name")
      .dropDuplicates(["Product_BK"])
)

# dim_customer_profile (key = CustomerProfile_BK)
dim_customer_profile = (
    df.select("CustomerProfile_BK", "Gender", "Income", "Customer_Segment")
      .dropDuplicates(["CustomerProfile_BK"])
)

# ============================================================
# 3) FACT TABLE (many side) — line-item grain (Silver grain)
# ============================================================
fact_sales = df.select(
    # Relationship keys
    "Transaction_BK",
    "transaction_date",
    "Country",
    "Product_BK",
    "CustomerProfile_BK",
    "Shipping_Method",
    "Order_Status",
    "Feedback_BK",
    # Measures
    "Total_Purchases",
    F.round(F.col("Amount"), 2).alias("Amount"),
    F.round(F.col("Total_Amount"), 2).alias("Total_Amount")
)

# ============================================================
# 4) WRITE Gold tables
# ============================================================
dim_date.write.format("delta").mode("overwrite").saveAsTable(f"{gold_schema}.dim_date")
dim_country.write.format("delta").mode("overwrite").saveAsTable(f"{gold_schema}.dim_country")
dim_product.write.format("delta").mode("overwrite").saveAsTable(f"{gold_schema}.dim_product")
dim_customer_profile.write.format("delta").mode("overwrite").saveAsTable(f"{gold_schema}.dim_customer_profile")
dim_shipping.write.format("delta").mode("overwrite").saveAsTable(f"{gold_schema}.dim_shipping")
dim_order_status.write.format("delta").mode("overwrite").saveAsTable(f"{gold_schema}.dim_order_status")
dim_feedback.write.format("delta").mode("overwrite").saveAsTable(f"{gold_schema}.dim_feedback")
fact_sales.write.format("delta").mode("overwrite").saveAsTable(f"{gold_schema}.fact_sales")

# ============================================================
# 5) Quick validation prints
# ============================================================
print("Gold Star Schema created (DROP + CREATE):")
for t in drop_tables:
    print("-", t)

print("\nCounts:")
print("dim_date:", spark.table(f"{gold_schema}.dim_date").count())
print("dim_country:", spark.table(f"{gold_schema}.dim_country").count())
print("dim_product:", spark.table(f"{gold_schema}.dim_product").count())
print("dim_customer_profile:", spark.table(f"{gold_schema}.dim_customer_profile").count())
print("dim_shipping:", spark.table(f"{gold_schema}.dim_shipping").count())
print("dim_order_status:", spark.table(f"{gold_schema}.dim_order_status").count())
print("dim_feedback:", spark.table(f"{gold_schema}.dim_feedback").count())
print("fact_sales:", spark.table(f"{gold_schema}.fact_sales").count())
