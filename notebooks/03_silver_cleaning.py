# ============================================================
# Layer: Silver
# Purpose:
#   Clean, validate, and standardize retail transaction data.
#   Apply data quality rules and prepare analytics-ready data.
#
# Key Transformations:
#   - Remove duplicates
#   - Handle nulls and invalid values
#   - Standardize categorical columns
#   - Validate numeric and date fields
#
# Input Table:
#   retail.bronze.retail_transactions
#
# Output Table:
#   retail.silver.retail_transactions_clean
#
# Author: Deepika Mandapalli
# ============================================================
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ============================================================
# Project 07 – Retail Data Engineering Pipeline
# Layer: Silver
# Purpose:
#   Clean, validate, and standardize retail transaction data
# ============================================================

# -----------------------------
# Config
# -----------------------------
source_table = "retail.bronze.retail_transactions"
target_table = "retail.silver.retail_transactions_clean"

spark.sql("CREATE SCHEMA IF NOT EXISTS retail.silver")

# -----------------------------
# Drop Silver table if exists
# -----------------------------
spark.sql(f"DROP TABLE IF EXISTS {target_table}")

# -----------------------------
# Read Bronze
# -----------------------------
df = spark.table(source_table)

# ============================================================
# 1) Drop unwanted columns (PII + not needed)
# ============================================================
drop_cols = [
    "Email",
    "Phone",
    "Address",
    "State",
    "City",
    "Zipcode",
    "Time",
    "Year",
    "Month",
    "Name"
]

df = df.drop(*[c for c in drop_cols if c in df.columns])

# ============================================================
# 2) Date cleanup → transaction_date (DATE)
# ============================================================
df = (
    df.withColumn(
        "transaction_date",
        F.coalesce(
            F.try_to_date("Date", "M/d/yyyy"),
            F.try_to_date("Date", "MM/dd/yyyy"),
            F.try_to_date("Date", "MM-dd-yy"),
            F.try_to_date("Date", "yyyy-MM-dd")
        )
    )
    .filter(F.col("transaction_date").isNotNull())
    .drop("Date")
)

# ============================================================
# 3) String cleanup
# ============================================================
string_cols = [
    f.name for f in df.schema.fields
    if isinstance(f.dataType, StringType)
]

for c in string_cols:
    df = df.withColumn(c, F.trim(F.col(c)))

# Country standardization
if "Country" in df.columns:
    df = df.withColumn(
        "Country",
        F.when(
            F.upper(F.col("Country")).isin(
                "USA", "US", "UNITED STATES", "UNITED STATES OF AMERICA"
            ),
            F.lit("USA")
        )
        .when(
            F.upper(F.col("Country")).isin(
                "UK", "U.K.", "UNITED KINGDOM", "GREAT BRITAIN"
            ),
            F.lit("UK")
        )
        .otherwise(F.initcap(F.col("Country")))
    )

# Normalize categoricals
cat_upper = [
    "Gender",
    "Income",
    "Customer_Segment",
    "Product_Category",
    "Product_Brand",
    "Product_Type",
    "Shipping_Method",
    "Order_Status",
    "Feedback"
]

for c in [c for c in cat_upper if c in df.columns]:
    df = df.withColumn(c, F.upper(F.col(c)))

# Product name formatting
if "products" in df.columns:
    df = (
        df.withColumn("products", F.initcap(F.col("products")))
          .withColumnRenamed("products", "Product_Name")
    )

# ============================================================
# 4) Preserve true grain
# ============================================================
df = df.dropDuplicates()

# ============================================================
# 5) Cast numerics + validations
# ============================================================
for c in ["Total_Purchases", "Amount", "Total_Amount"]:
    if c in df.columns:
        df = df.withColumn(c, F.col(c).cast("double"))

for c in ["Total_Purchases", "Amount", "Total_Amount"]:
    if c in df.columns:
        df = df.filter(F.col(c) >= 0)

# Age sanity
if "Age" in df.columns:
    df = (
        df.withColumn("Age", F.col("Age").cast("int"))
          .filter((F.col("Age") >= 0) & (F.col("Age") <= 100))
    )

# Ratings validation
if "Ratings" in df.columns:
    df = df.withColumn("Ratings", F.col("Ratings").cast("int"))
    df = df.withColumn(
        "Ratings",
        F.when(
            (F.col("Ratings") >= 1) & (F.col("Ratings") <= 5),
            F.col("Ratings")
        ).otherwise(F.lit(None).cast("int"))
    )

# ============================================================
# 6) Null handling aligned to business objectives
# ============================================================
required_cols = [
    "Transaction_ID",
    "Country",
    "transaction_date",
    "Gender",
    "Income",
    "Customer_Segment",
    "Product_Category",
    "Product_Brand",
    "Product_Name",
    "Total_Amount",
    "Shipping_Method",
    "Order_Status",
    "Feedback"
]

df = df.dropna(subset=[c for c in required_cols if c in df.columns])

fill_unknown_cols = [
    "Product_Type",
    "Gender",
    "Income",
    "Customer_Segment",
    "Product_Category",
    "Product_Brand",
    "Product_Name",
    "Shipping_Method",
    "Order_Status"
]

df = df.fillna("UNKNOWN", subset=[c for c in fill_unknown_cols if c in df.columns])

# ============================================================
# 7) Add Transaction_BK (deterministic)
# ============================================================
if "Transaction_ID" in df.columns and "Country" in df.columns:
    df = df.withColumn(
        "Transaction_BK",
        F.concat_ws("|", F.col("Transaction_ID").cast("string"), F.col("Country"))
    )

# ============================================================
# 8) Write Silver table
# ============================================================
df.write.format("delta").mode("overwrite").saveAsTable(target_table)

print("Silver table created:", target_table)
print("Row count:", df.count())
df.printSchema()
