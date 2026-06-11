"""
utils/config.py

Shared project settings.

Keep paths, file names, schemas, and basic validation rules here so the
pipeline code does not have hardcoded values everywhere.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)

# Databricks storage paths.
# Change only this base path if you move the project to a new location.
base_path = "/Volumes/workspace/default/big_mart_sales"

raw_path = base_path
bronze_path = f"{base_path}/bronze"
silver_path = f"{base_path}/silver"
gold_path = f"{base_path}/gold"


# Source CSV files from the Olist dataset.
csv_files = {
    "orders": "olist_orders_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "products": "olist_products_dataset.csv",
    "items": "olist_order_items_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "translations": "product_category_name_translation.csv",
}


# Explicit schemas make ingestion more predictable than inferSchema.
# The "lenght" spelling is kept because that is how it appears in the source file.
SCHEMA_ORDERS = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", TimestampType(), True),
    StructField("order_approved_at", TimestampType(), True),
    StructField("order_delivered_carrier_date", TimestampType(), True),
    StructField("order_delivered_customer_date", TimestampType(), True),
    StructField("order_estimated_delivery_date", TimestampType(), True),
])

SCHEMA_CUSTOMERS = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_unique_id", StringType(), False),
    StructField("customer_zip_code_prefix", IntegerType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
])

SCHEMA_ITEMS = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_item_id", IntegerType(), False),
    StructField("product_id", StringType(), False),
    StructField("seller_id", StringType(), False),
    StructField("shipping_limit_date", TimestampType(), True),
    StructField("price", DoubleType(), True),
    StructField("freight_value", DoubleType(), True),
])

SCHEMA_PAYMENTS = StructType([
    StructField("order_id", StringType(), False),
    StructField("payment_sequential", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DoubleType(), True),
])

SCHEMA_REVIEWS = StructType([
    StructField("review_id", StringType(), True),
    StructField("order_id", StringType(), False),
    StructField("review_score", IntegerType(), True),
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", TimestampType(), True),
    StructField("review_answer_timestamp", TimestampType(), True),
])

SCHEMA_PRODUCTS = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_category_name", StringType(), True),
    StructField("product_name_lenght", IntegerType(), True),
    StructField("product_description_lenght", IntegerType(), True),
    StructField("product_photos_qty", IntegerType(), True),
    StructField("product_weight_g", IntegerType(), True),
    StructField("product_length_cm", IntegerType(), True),
    StructField("product_height_cm", IntegerType(), True),
    StructField("product_width_cm", IntegerType(), True),
])

SCHEMA_SELLERS = StructType([
    StructField("seller_id", StringType(), False),
    StructField("seller_zip_code_prefix", IntegerType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True),
])

SCHEMA_GEOLOCATION = StructType([
    StructField("geolocation_zip_code_prefix", IntegerType(), True),
    StructField("geolocation_lat", DoubleType(), True),
    StructField("geolocation_lng", DoubleType(), True),
    StructField("geolocation_city", StringType(), True),
    StructField("geolocation_state", StringType(), True),
])

SCHEMA_TRANSLATIONS = StructType([
    StructField("product_category_name", StringType(), True),
    StructField("product_category_name_english", StringType(), True),
])


# Used by the Bronze loader to read all files in a simple loop.
ALL_SCHEMAS = {
    "orders": SCHEMA_ORDERS,
    "customers": SCHEMA_CUSTOMERS,
    "items": SCHEMA_ITEMS,
    "payments": SCHEMA_PAYMENTS,
    "reviews": SCHEMA_REVIEWS,
    "products": SCHEMA_PRODUCTS,
    "sellers": SCHEMA_SELLERS,
    "geolocation": SCHEMA_GEOLOCATION,
    "translations": SCHEMA_TRANSLATIONS,
}


# Basic checks on important join keys.
# If these are missing, the Silver joins will produce bad results.
NOT_NULL_COLUMNS = {
    "orders": ["order_id", "customer_id"],
    "customers": ["customer_id", "customer_unique_id"],
    "items": ["order_id", "product_id", "seller_id"],
    "payments": ["order_id"],
    "reviews": ["order_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
}


# Rough row count checks to catch missing or partially uploaded files.
MIN_ROW_COUNTS = {
    "orders": 95_000,
    "customers": 95_000,
    "items": 100_000,
    "payments": 100_000,
    "reviews": 95_000,
    "products": 30_000,
    "sellers": 3_000,
}