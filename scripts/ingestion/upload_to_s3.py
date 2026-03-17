import os
import sys
import logging
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "dylan-ecommerce-data-pipeline")
S3_PREFIX = os.getenv("S3_BRONZE_PREFIX", "bronze")
LOCAL_SOURCE_DIR = Path(os.getenv("LOCAL_SOURCE_DIR", "data/source"))

TABLE_FILES = {
    "customers": "customers.csv",
    "orders": "orders.csv",
    "order_items": "order_items.csv",
    "order_payments": "order_payments.csv",
    "order_reviews": "order_reviews.csv",
    "products": "products.csv",
    "sellers": "sellers.csv",
    "product_category_name_translation": "product_category_name_translation.csv",
}

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "upload_to_s3.log", encoding="utf-8"),
    ],
)

logger = logging.getLogger(__name__)


def build_s3_key(table_name: str, file_name: str) -> str:
    return f"{S3_PREFIX}/{table_name}/{file_name}"


def validate_local_file(file_path: Path) -> bool:
    if not file_path.exists():
        logger.error("Missing file: %s", file_path)
        return False
    if file_path.stat().st_size == 0:
        logger.error("Empty file: %s", file_path)
        return False
    return True


def upload_file_to_s3(s3_client, local_path: Path, bucket_name: str, s3_key: str) -> bool:
    try:
        s3_client.upload_file(str(local_path), bucket_name, s3_key)
        logger.info("Uploaded: %s -> s3://%s/%s", local_path, bucket_name, s3_key)
        return True
    except FileNotFoundError:
        logger.exception("Local file not found: %s", local_path)
        return False
    except NoCredentialsError:
        logger.exception("AWS credentials not found.")
        return False
    except ClientError:
        logger.exception("AWS client error while uploading %s", local_path)
        return False
    except Exception:
        logger.exception("Unexpected error while uploading %s", local_path)
        return False


def main() -> int:
    logger.info("Starting Bronze ingestion to S3...")
    logger.info("Bucket: %s", BUCKET_NAME)
    logger.info("Prefix: %s", S3_PREFIX)
    logger.info("Source dir: %s", LOCAL_SOURCE_DIR.resolve())

    if not LOCAL_SOURCE_DIR.exists():
        logger.error("Source directory does not exist: %s", LOCAL_SOURCE_DIR.resolve())
        return 1

    s3_client = boto3.client("s3")

    uploaded_count = 0
    failed_count = 0

    for table_name, file_name in TABLE_FILES.items():
        local_file_path = LOCAL_SOURCE_DIR / file_name
        s3_key = build_s3_key(table_name, file_name)

        if not validate_local_file(local_file_path):
            failed_count += 1
            continue

        if upload_file_to_s3(s3_client, local_file_path, BUCKET_NAME, s3_key):
            uploaded_count += 1
        else:
            failed_count += 1

    logger.info("Finished Bronze ingestion. Uploaded=%s Failed=%s", uploaded_count, failed_count)
    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())