"""
S3 upload functionality for customer and transaction data.

This module provides functions to upload generated CSV files to AWS S3
with retry logic and progress tracking.
"""

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from pathlib import Path
from typing import Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((ClientError, ConnectionError)),
    reraise=True
)
def upload_to_s3(
    local_file: str,
    s3_bucket: str,
    s3_key: str,
    aws_profile: Optional[str] = None
) -> bool:
    """
    Upload a file to S3 with retry logic.

    Args:
        local_file: Path to local file to upload
        s3_bucket: Name of S3 bucket
        s3_key: S3 object key (path within bucket)
        aws_profile: Optional AWS profile name to use

    Returns:
        bool: True if upload successful, False otherwise

    Raises:
        FileNotFoundError: If local file doesn't exist
        NoCredentialsError: If AWS credentials not configured
        ClientError: If S3 upload fails after retries

    Example:
        >>> success = upload_to_s3(
        ...     'data/customers.csv',
        ...     'my-bucket',
        ...     'customers/customers.csv'
        ... )
        >>> print(success)
        True
    """
    # Verify file exists
    file_path = Path(local_file)
    if not file_path.exists():
        raise FileNotFoundError(f"Local file not found: {local_file}")

    file_size_mb = file_path.stat().st_size / (1024 * 1024)

    try:
        # Create S3 client
        if aws_profile:
            session = boto3.Session(profile_name=aws_profile)
            s3_client = session.client('s3')
        else:
            s3_client = boto3.client('s3')

        logger.info(f"Uploading {local_file} ({file_size_mb:.2f} MB) to s3://{s3_bucket}/{s3_key}")

        # Upload file
        s3_client.upload_file(
            Filename=str(file_path),
            Bucket=s3_bucket,
            Key=s3_key
        )

        logger.info(f"✓ Upload successful: s3://{s3_bucket}/{s3_key}")
        return True

    except NoCredentialsError:
        logger.error("✗ AWS credentials not found. Please configure credentials.")
        raise

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"✗ S3 upload failed: {error_code} - {error_message}")
        raise

    except Exception as e:
        logger.error(f"✗ Unexpected error during upload: {str(e)}")
        return False


def upload_customers_to_s3(
    csv_file: str,
    s3_bucket: str,
    aws_profile: Optional[str] = None
) -> bool:
    """
    Upload customer CSV file to S3 customers/ folder.

    Args:
        csv_file: Path to customer CSV file
        s3_bucket: Name of S3 bucket
        aws_profile: Optional AWS profile name

    Returns:
        bool: True if upload successful, False otherwise

    Example:
        >>> success = upload_customers_to_s3(
        ...     'data/customers.csv',
        ...     'customer360-analytics-data-20250111'
        ... )
    """
    file_path = Path(csv_file)
    s3_key = f"customers/{file_path.name}"

    try:
        return upload_to_s3(
            local_file=csv_file,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            aws_profile=aws_profile
        )
    except Exception as e:
        logger.error(f"Failed to upload customers: {str(e)}")
        return False


def upload_transactions_to_s3(
    csv_file: str,
    s3_bucket: str,
    folder: str = "historical",
    aws_profile: Optional[str] = None
) -> bool:
    """
    Upload transaction CSV file to S3 transactions/ folder.

    Args:
        csv_file: Path to transaction CSV file
        s3_bucket: Name of S3 bucket
        folder: Subfolder under transactions/ (historical or streaming)
        aws_profile: Optional AWS profile name

    Returns:
        bool: True if upload successful, False otherwise

    Example:
        >>> success = upload_transactions_to_s3(
        ...     'data/transactions.csv',
        ...     'customer360-analytics-data-20250111',
        ...     folder='historical'
        ... )
    """
    if folder not in ["historical", "streaming"]:
        raise ValueError(f"Invalid folder: {folder}. Must be 'historical' or 'streaming'")

    file_path = Path(csv_file)
    s3_key = f"transactions/{folder}/{file_path.name}"

    try:
        return upload_to_s3(
            local_file=csv_file,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            aws_profile=aws_profile
        )
    except Exception as e:
        logger.error(f"Failed to upload transactions: {str(e)}")
        return False


def list_s3_files(
    s3_bucket: str,
    prefix: str = "",
    aws_profile: Optional[str] = None
) -> list:
    """
    List files in S3 bucket with optional prefix.

    Args:
        s3_bucket: Name of S3 bucket
        prefix: S3 key prefix to filter (e.g., "customers/")
        aws_profile: Optional AWS profile name

    Returns:
        list: List of S3 object keys

    Example:
        >>> files = list_s3_files('my-bucket', 'customers/')
        >>> print(files)
        ['customers/customers.csv']
    """
    try:
        if aws_profile:
            session = boto3.Session(profile_name=aws_profile)
            s3_client = session.client('s3')
        else:
            s3_client = boto3.client('s3')

        response = s3_client.list_objects_v2(
            Bucket=s3_bucket,
            Prefix=prefix
        )

        if 'Contents' not in response:
            return []

        return [obj['Key'] for obj in response['Contents']]

    except ClientError as e:
        logger.error(f"Failed to list S3 files: {e}")
        return []


def verify_s3_upload(
    s3_bucket: str,
    s3_key: str,
    aws_profile: Optional[str] = None
) -> bool:
    """
    Verify that a file exists in S3.

    Args:
        s3_bucket: Name of S3 bucket
        s3_key: S3 object key
        aws_profile: Optional AWS profile name

    Returns:
        bool: True if file exists, False otherwise

    Example:
        >>> exists = verify_s3_upload('my-bucket', 'customers/customers.csv')
        >>> print(exists)
        True
    """
    try:
        if aws_profile:
            session = boto3.Session(profile_name=aws_profile)
            s3_client = session.client('s3')
        else:
            s3_client = boto3.client('s3')

        s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
        logger.info(f"✓ Verified: s3://{s3_bucket}/{s3_key} exists")
        return True

    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.warning(f"✗ File not found: s3://{s3_bucket}/{s3_key}")
            return False
        else:
            logger.error(f"Error verifying file: {e}")
            return False
