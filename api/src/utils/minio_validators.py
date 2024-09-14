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

async def check_directory_exists(bucket_name: str, directory_name: str):
    """
    Checks if a directory exists in the S3 bucket by checking for any objects with the directory prefix.
    If no objects are found with the prefix, it is assumed that the directory does not exist.

    :param bucket_name: The name of the S3 bucket.
    :param directory_name: The name of the directory to check.
    :raises HTTPException: If the directory does not exist.
    """
    if not directory_name.endswith('/'):
        directory_name += '/'

    try:
        objects = client.list_objects(bucket_name, prefix=directory_name, recursive=False)
        first_object = next(objects, None)
        if not first_object:
            logger.error(f"Directory '{directory_name}' does not exist in bucket '{bucket_name}'.")
            raise HTTPException(status_code=404, detail=f"Directory '{directory_name}' does not exist in bucket '{bucket_name}'.")
    except StopIteration:
        logger.error(f"Directory '{directory_name}' does not exist in bucket '{bucket_name}'.")
        raise HTTPException(status_code=404, detail=f"Directory '{directory_name}' does not exist in bucket '{bucket_name}'.")
    except Exception as e:
        logger.error(f"Failed to check if directory '{directory_name}' exists in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to check directory existence: {str(e)}")

async def check_class_exists(bucket_name: str, directory_name: str, class_name: str):
    """
    Checks if a class (subfolder) exists in a directory within the S3 bucket by checking for any objects with the class prefix.
    If no objects are found with the prefix, it is assumed that the class does not exist.

    :param bucket_name: The name of the S3 bucket.
    :param directory_name: The name of the directory.
    :param class_name: The name of the class (subfolder) to check.
    :raises HTTPException: If the class does not exist.
    """
    if not directory_name.endswith('/'):
        directory_name += '/'
    if not class_name.endswith('/'):
        class_name += '/'
    full_prefix = f"{directory_name}{class_name}"

    try:
        objects = client.list_objects(bucket_name, prefix=full_prefix, recursive=False)
        first_object = next(objects, None)
        if not first_object:
            logger.error(f"Class '{class_name}' does not exist in directory '{directory_name}' of bucket '{bucket_name}'.")
            raise HTTPException(status_code=404, detail=f"Class '{class_name}' does not exist in directory '{directory_name}' of bucket '{bucket_name}'.")
    except StopIteration:
        logger.error(f"Class '{class_name}' does not exist in directory '{directory_name}' of bucket '{bucket_name}'.")
        raise HTTPException(status_code=404, detail=f"Class '{class_name}' does not exist in directory '{directory_name}' of bucket '{bucket_name}'.")
    except Exception as e:
        logger.error(f"Failed to check if class '{class_name}' exists in directory '{directory_name}' of bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to check class existence: {str(e)}")