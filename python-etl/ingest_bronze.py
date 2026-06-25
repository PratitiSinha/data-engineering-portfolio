"""
ingest_bronze.py
----------------
Extract: Download NYC TLC Yellow Taxi data from public URL
Load:    Upload raw file to ADLS Gen2 bronze layer

This is the first step of the medallion pipeline.
Bronze = raw, untouched data exactly as received from source.
"""

import os
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# ── Setup logging ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ── Load credentials from .env ─────────────────────────────────────────────
load_dotenv()

ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
ACCOUNT_KEY  = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
CONTAINER    = "medallion"

# ── Config ─────────────────────────────────────────────────────────────────
YEAR  = 2024
MONTH = 1  # January 2024

# NYC TLC public URL for yellow taxi data
SOURCE_URL = (
    f"https://d37ci6vzurychx.cloudfront.net/trip-data/"
    f"yellow_tripdata_{YEAR}-{MONTH:02d}.parquet"
)

# Bronze destination path (partitioned by year/month)
BRONZE_PATH = (
    f"bronze/raw/yellow_taxi/"
    f"year={YEAR}/month={MONTH:02d}/"
    f"yellow_tripdata_{YEAR}-{MONTH:02d}.parquet"
)


def get_blob_client() -> BlobServiceClient:
    """
    Create and return an authenticated BlobServiceClient.
    Reads credentials from environment variables.
    """
    if not ACCOUNT_NAME or not ACCOUNT_KEY:
        raise ValueError(
            "Missing Azure credentials. "
            "Check AZURE_STORAGE_ACCOUNT_NAME and "
            "AZURE_STORAGE_ACCOUNT_KEY in your .env file."
        )

    account_url = f"https://{ACCOUNT_NAME}.blob.core.windows.net"
    logger.info(f"Connecting to storage account: {ACCOUNT_NAME}")

    return BlobServiceClient(
        account_url=account_url,
        credential=ACCOUNT_KEY
    )


def download_from_source(url: str) -> bytes:
    """
    Download data from a public URL.
    Returns raw bytes of the file.
    """
    logger.info(f"Downloading from source: {url}")

    response = requests.get(url, stream=True, timeout=300, headers={
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
})

    if response.status_code != 200:
        raise Exception(
            f"Download failed. "
            f"HTTP {response.status_code}: {response.reason}"
        )

    data = response.content
    size_mb = len(data) / (1024 * 1024)
    logger.info(f"Downloaded {size_mb:.2f} MB successfully")

    return data


def upload_to_bronze(
    blob_client: BlobServiceClient,
    data: bytes,
    blob_path: str
) -> None:
    """
    Upload raw data to the bronze layer in ADLS Gen2.
    Overwrites if file already exists.
    """
    logger.info(f"Uploading to bronze: {blob_path}")

    container_client = blob_client.get_container_client(CONTAINER)
    blob = container_client.get_blob_client(blob_path)

    blob.upload_blob(data, overwrite=True)

    logger.info(f"Upload complete: {CONTAINER}/{blob_path}")


def run_ingestion() -> None:
    """
    Main orchestration function.
    Extract from source → Load to bronze.
    """
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("NYC Taxi Bronze Ingestion — START")
    logger.info(f"Target: {YEAR}-{MONTH:02d}")
    logger.info("=" * 60)

    try:
        # Step 1: Connect to Azure
        blob_client = get_blob_client()

        # Step 2: Download from source
        raw_data = download_from_source(SOURCE_URL)

        # Step 3: Upload to bronze layer
        upload_to_bronze(blob_client, raw_data, BRONZE_PATH)

        # Success summary
        duration = (datetime.now() - start_time).seconds
        logger.info("=" * 60)
        logger.info(f"Ingestion COMPLETE in {duration}s")
        logger.info(f"Bronze path: {CONTAINER}/{BRONZE_PATH}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Ingestion FAILED: {e}")
        raise


# ── Entry point ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_ingestion()