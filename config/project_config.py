# Project 07 – Global Configuration

CATALOG = "retail"

SCHEMA_LANDING = "landing_zone"
SCHEMA_BRONZE  = "bronze"
SCHEMA_SILVER  = "silver"
SCHEMA_GOLD    = "gold"

LANDING_TABLE = f"{CATALOG}.{SCHEMA_LANDING}.retail_data"
BRONZE_TABLE  = f"{CATALOG}.{SCHEMA_BRONZE}.retail_transactions"
SILVER_TABLE  = f"{CATALOG}.{SCHEMA_SILVER}.retail_transactions_clean"

DIM_DATE      = f"{CATALOG}.{SCHEMA_GOLD}.dim_date"
DIM_COUNTRY   = f"{CATALOG}.{SCHEMA_GOLD}.dim_country"
DIM_PRODUCT   = f"{CATALOG}.{SCHEMA_GOLD}.dim_product"
DIM_CUSTOMER  = f"{CATALOG}.{SCHEMA_GOLD}.dim_customer_profile"
DIM_SHIPPING  = f"{CATALOG}.{SCHEMA_GOLD}.dim_shipping"
DIM_STATUS    = f"{CATALOG}.{SCHEMA_GOLD}.dim_order_status"
DIM_FEEDBACK  = f"{CATALOG}.{SCHEMA_GOLD}.dim_feedback"
FACT_SALES    = f"{CATALOG}.{SCHEMA_GOLD}.fact_sales"
