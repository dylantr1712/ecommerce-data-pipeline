import os
import sys
import logging
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

BUCKET_NAME = (
    os.getenv("AWS_S3_BUCKET_NAME")
    or os.getenv("S3_BUCKET_NAME")
    or os.getenv("S3_BUCKET")
    or "dylan-ecommerce-data-pipeline"
)
S3_PREFIX = os.getenv("S3_SCRIPTS_PREFIX", "scripts")
LOCAL_SCRIPT_PATH = Path(
    os.getenv("LOCAL_SPARK_SCRIPT", "scripts/spark/bronze_to_silver.py")
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def main() -> int:
    logger.info("Uploading Spark script to S3...")
    logger.info("Bucket: %s", BUCKET_NAME)
    logger.info("Prefix: %s", S3_PREFIX)
    logger.info("Local script: %s", LOCAL_SCRIPT_PATH)

    if not LOCAL_SCRIPT_PATH.exists():
        logger.error("Local script not found: %s", LOCAL_SCRIPT_PATH)
        return 1

    s3_key = f"{S3_PREFIX}/{LOCAL_SCRIPT_PATH.name}"

    s3_client = boto3.client("s3")

    try:
        s3_client.upload_file(str(LOCAL_SCRIPT_PATH), BUCKET_NAME, s3_key)
        logger.info("Uploaded: %s -> s3://%s/%s", LOCAL_SCRIPT_PATH, BUCKET_NAME, s3_key)
        return 0
    except FileNotFoundError:
        logger.exception("Local file not found: %s", LOCAL_SCRIPT_PATH)
        return 1
    except NoCredentialsError:
        logger.exception("AWS credentials not found.")
        return 1
    except ClientError:
        logger.exception("AWS client error while uploading %s", LOCAL_SCRIPT_PATH)
        return 1
    except Exception:
        logger.exception("Unexpected error while uploading %s", LOCAL_SCRIPT_PATH)
        return 1


if __name__ == "__main__":
    sys.exit(main())
