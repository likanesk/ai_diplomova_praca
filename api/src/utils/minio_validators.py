import logging
from fastapi import HTTPException
from minio.error import S3Error
from .minio_client import get_minio_client

client = get_minio_client()

logger = logging.getLogger(__name__)

async def check_bucket_exists(bucket_name: str):
    """
    Checks if the S3 bucket exists, raises an HTTPException if not found.
    """
    if not client.bucket_exists(bucket_name):
        logger.error(f"Bucket '{bucket_name}' does not exist.")
        raise HTTPException(status_code=404, detail=f"Bucket '{bucket_name}' does not exist.")

async def check_file_exists(bucket_name: str, file_name: str):
    """
    Checks if the file exists in the given S3 bucket, raises an HTTPException if not found.
    """
    try:
        client.stat_object(bucket_name, file_name)
    except S3Error as e:
        if "NoSuchKey" in str(e):
            logger.error(f"File '{file_name}' does not exist in bucket '{bucket_name}'.")
            raise HTTPException(status_code=404, detail=f"File '{file_name}' does not exist in bucket '{bucket_name}'.")
        logger.error(f"Error occurred while checking for file '{file_name}' in bucket '{bucket_name}': {str(e)}")
        raise e
