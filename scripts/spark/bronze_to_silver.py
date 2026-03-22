import os
import sys
import logging
from typing import List, Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    upper,
    current_timestamp,
    input_file_name,
    to_timestamp,
    regexp_replace,
    when,
    lit
)
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DecimalType
)

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------

BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "dylan-ecommerce-data-pipeline")
BRONZE_PREFIX = os.getenv("S3_BRONZE_PREFIX", "bronze")
SILVER_PREFIX = os.getenv("S3_SILVER_PREFIX", "silver")

TABLES = [
    "customers",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "sellers",
    "product_category_name_translation",
]

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

def get_bronze_path(table_name: str) -> str:
    return f"s3://{BUCKET_NAME}/{BRONZE_PREFIX}/{table_name}/"

def get_silver_path(table_name: str) -> str:
    return f"s3://{BUCKET_NAME}/{SILVER_PREFIX}/{table_name}/"

def standardize_column_names(df: DataFrame) -> DataFrame:
    """
    Lowercase column names and strip surrounding spaces.
    """
    new_cols = [c.strip().lower() for c in df.columns]
    return df.toDF(*new_cols)

def trim_all_string_columns(df: DataFrame) -> DataFrame:
    """
    Trim whitespace from all string columns.
    """
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, trim(col(field.name)))
    return df

def add_metadata_columns(df: DataFrame) -> DataFrame:
    """
    Add ingestion metadata columns.
    """
    return (
        df.withColumn("ingestion_timestamp", current_timestamp())
          .withColumn("source_file_name", input_file_name())
    )

def safe_cast_int(df: DataFrame, columns: List[str]) -> DataFrame:
    for c in columns:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(IntegerType()))
    return df

def safe_cast_decimal(df: DataFrame, columns: List[str], precision: int = 12, scale: int = 2) -> DataFrame:
    for c in columns:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(DecimalType(precision, scale)))
    return df

def safe_cast_string(df: DataFrame, columns: List[str]) -> DataFrame:
    for c in columns:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(StringType()))
    return df

def safe_cast_timestamp(df: DataFrame, columns: List[str], fmt: str = "yyyy-MM-dd HH:mm:ss") -> DataFrame:
    for c in columns:
        if c in df.columns:
            df = df.withColumn(c, to_timestamp(col(c), fmt))
    return df

def lowercase_columns(df: DataFrame, columns: List[str]) -> DataFrame:
    for c in columns:
        if c in df.columns:
            df = df.withColumn(c, lower(col(c)))
    return df

def uppercase_columns(df: DataFrame, columns: List[str]) -> DataFrame:
    for c in columns:
        if c in df.columns:
            df = df.withColumn(c, upper(col(c)))
    return df

def normalize_text_columns(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Trim text and collapse multiple spaces.
    """
    for c in columns:
        if c in df.columns:
            df = df.withColumn(c, regexp_replace(trim(col(c)), r"\s+", " "))
    return df

def filter_required_not_null(df: DataFrame, required_cols: List[str]) -> DataFrame:
    for c in required_cols:
        if c in df.columns:
            df = df.filter(col(c).isNotNull())
    return df

# ------------------------------------------------------------------------------
# Table-specific transformations
# ------------------------------------------------------------------------------

def transform_customers(df: DataFrame) -> DataFrame:
    df = standardize_column_names(df)
    df = trim_all_string_columns(df)

    df = safe_cast_string(df, [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix"
    ])

    df = normalize_text_columns(df, ["customer_city"])
    df = uppercase_columns(df, ["customer_state"])

    df = filter_required_not_null(df, ["customer_id", "customer_unique_id"])
    df = df.dropDuplicates(["customer_id"])

    return add_metadata_columns(df)

def transform_orders(df: DataFrame) -> DataFrame:
    df = standardize_column_names(df)
    df = trim_all_string_columns(df)

    df = safe_cast_string(df, ["order_id", "customer_id"])
    df = lowercase_columns(df, ["order_status"])

    df = safe_cast_timestamp(df, [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ])

    df = filter_required_not_null(df, ["order_id", "customer_id"])
    df = df.dropDuplicates(["order_id"])

    return add_metadata_columns(df)

def transform_order_items(df: DataFrame) -> DataFrame:
    df = standardize_column_names(df)
    df = trim_all_string_columns(df)

    df = safe_cast_string(df, ["order_id", "product_id", "seller_id"])
    df = safe_cast_int(df, ["order_item_id"])
    df = safe_cast_decimal(df, ["price", "freight_value"])
    df = safe_cast_timestamp(df, ["shipping_limit_date"])

    df = filter_required_not_null(df, ["order_id", "order_item_id", "product_id", "seller_id"])

    if "price" in df.columns:
        df = df.filter(col("price").isNull() | (col("price") >= 0))
    if "freight_value" in df.columns:
        df = df.filter(col("freight_value").isNull() | (col("freight_value") >= 0))

    df = df.dropDuplicates(["order_id", "order_item_id", "product_id", "seller_id"])

    return add_metadata_columns(df)

def transform_order_payments(df: DataFrame) -> DataFrame:
    df = standardize_column_names(df)
    df = trim_all_string_columns(df)

    df = safe_cast_string(df, ["order_id"])
    df = lowercase_columns(df, ["payment_type"])

    df = safe_cast_int(df, ["payment_sequential", "payment_installments"])
    df = safe_cast_decimal(df, ["payment_value"])

    df = filter_required_not_null(df, ["order_id", "payment_sequential"])

    if "payment_value" in df.columns:
        df = df.filter(col("payment_value").isNull() | (col("payment_value") >= 0))
    if "payment_installments" in df.columns:
        df = df.filter(col("payment_installments").isNull() | (col("payment_installments") >= 0))

    df = df.dropDuplicates(["order_id", "payment_sequential"])

    return add_metadata_columns(df)

def transform_order_reviews(df: DataFrame) -> DataFrame:
    df = standardize_column_names(df)
    df = trim_all_string_columns(df)

    df = safe_cast_string(df, ["review_id", "order_id"])
    df = safe_cast_int(df, ["review_score"])
    df = safe_cast_timestamp(df, ["review_creation_date", "review_answer_timestamp"])

    df = normalize_text_columns(df, ["review_comment_title", "review_comment_message"])

    df = filter_required_not_null(df, ["review_id", "order_id"])

    if "review_score" in df.columns:
        df = df.filter(col("review_score").between(1, 5))

    df = df.dropDuplicates(["review_id"])

    return add_metadata_columns(df)

def transform_products(df: DataFrame) -> DataFrame:
    df = standardize_column_names(df)
    df = trim_all_string_columns(df)

    df = safe_cast_string(df, ["product_id", "product_category_name"])
    df = safe_cast_int(df, [
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ])

    df = normalize_text_columns(df, ["product_category_name"])
    df = filter_required_not_null(df, ["product_id"])
    df = df.dropDuplicates(["product_id"])

    # Remove obviously invalid negative measures
    numeric_measure_cols = [
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]
    for c in numeric_measure_cols:
        if c in df.columns:
            df = df.filter(col(c).isNull() | (col(c) >= 0))

    return add_metadata_columns(df)

def transform_sellers(df: DataFrame) -> DataFrame:
    df = standardize_column_names(df)
    df = trim_all_string_columns(df)

    df = safe_cast_string(df, ["seller_id", "seller_zip_code_prefix"])
    df = normalize_text_columns(df, ["seller_city"])
    df = uppercase_columns(df, ["seller_state"])

    df = filter_required_not_null(df, ["seller_id"])
    df = df.dropDuplicates(["seller_id"])

    return add_metadata_columns(df)

def transform_product_category_name_translation(df: DataFrame) -> DataFrame:
    df = standardize_column_names(df)
    df = trim_all_string_columns(df)

    df = safe_cast_string(df, [
        "product_category_name",
        "product_category_name_english"
    ])

    df = normalize_text_columns(df, [
        "product_category_name",
        "product_category_name_english"
    ])

    df = filter_required_not_null(df, ["product_category_name"])
    df = df.dropDuplicates(["product_category_name"])

    return add_metadata_columns(df)

TRANSFORMERS: Dict[str, callable] = {
    "customers": transform_customers,
    "orders": transform_orders,
    "order_items": transform_order_items,
    "order_payments": transform_order_payments,
    "order_reviews": transform_order_reviews,
    "products": transform_products,
    "sellers": transform_sellers,
    "product_category_name_translation": transform_product_category_name_translation,
}

# ------------------------------------------------------------------------------
# Main processing
# ------------------------------------------------------------------------------

def process_table(spark: SparkSession, table_name: str) -> None:
    bronze_path = get_bronze_path(table_name)
    silver_path = get_silver_path(table_name)

    logger.info("Processing table: %s", table_name)
    logger.info("Reading Bronze: %s", bronze_path)

    df = (
        spark.read
        .option("header", True)
        .option("multiLine", False)
        .option("escape", "\"")
        .csv(bronze_path)
    )

    raw_count = df.count()
    logger.info("Raw record count for %s: %s", table_name, raw_count)

    transformer = TRANSFORMERS[table_name]
    df_clean = transformer(df)

    clean_count = df_clean.count()
    logger.info("Clean record count for %s: %s", table_name, clean_count)

    logger.info("Writing Silver Parquet: %s", silver_path)
    (
        df_clean.write
        .mode("overwrite")
        .parquet(silver_path)
    )

    logger.info("Finished table: %s", table_name)

def main() -> int:
    logger.info("Starting Bronze -> Silver Spark job")
    logger.info("Bucket=%s BronzePrefix=%s SilverPrefix=%s", BUCKET_NAME, BRONZE_PREFIX, SILVER_PREFIX)

    spark = (
        SparkSession.builder
        .appName("BronzeToSilverEcommerce")
        .getOrCreate()
    )

    try:
        for table_name in TABLES:
            process_table(spark, table_name)

        logger.info("Bronze -> Silver completed successfully")
        return 0

    except Exception:
        logger.exception("Bronze -> Silver job failed")
        return 1

    finally:
        spark.stop()

if __name__ == "__main__":
    sys.exit(main())