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

async def check_database_exists(bucket_name: str, database_name: str):
    """
    Checks if a database exists in the S3 bucket by checking for any objects with the database prefix.
    If no objects are found with the prefix, it is assumed that the database does not exist.

    :param bucket_name: The name of the S3 bucket.
    :param database_name: The name of the database to check.
    :raises HTTPException: If the database does not exist.
    """
    if not database_name.endswith('/'):
        database_name += '/'

    try:
        objects = client.list_objects(bucket_name, prefix=database_name, recursive=False)
        first_object = next(objects, None)
        if not first_object:
            logger.error(f"Database '{database_name}' does not exist in bucket '{bucket_name}'.")
            raise HTTPException(status_code=404, detail=f"Database '{database_name}' does not exist in bucket '{bucket_name}'.")
    except StopIteration:
        logger.error(f"Database '{database_name}' does not exist in bucket '{bucket_name}'.")
        raise HTTPException(status_code=404, detail=f"Database '{database_name}' does not exist in bucket '{bucket_name}'.")
    except Exception as e:
        logger.error(f"Failed to check if database '{database_name}' exists in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to check database existence: {str(e)}")

async def check_class_exists(bucket_name: str, database_name: str, class_name: str):
    """
    Checks if a class (subfolder) exists in a database within the S3 bucket by checking for any objects with the class prefix.
    If no objects are found with the prefix, it is assumed that the class does not exist.

    :param bucket_name: The name of the S3 bucket.
    :param database_name: The name of the database.
    :param class_name: The name of the class (subfolder) to check.
    :raises HTTPException: If the class does not exist.
    """
    if not database_name.endswith('/'):
        database_name += '/'
    if not class_name.endswith('/'):
        class_name += '/'
    full_prefix = f"{database_name}{class_name}"

    try:
        objects = client.list_objects(bucket_name, prefix=full_prefix, recursive=False)
        first_object = next(objects, None)
        if not first_object:
            logger.error(f"Class '{class_name}' does not exist in database '{database_name}' of bucket '{bucket_name}'.")
            raise HTTPException(status_code=404, detail=f"Class '{class_name}' does not exist in database '{database_name}' of bucket '{bucket_name}'.")
    except StopIteration:
        logger.error(f"Class '{class_name}' does not exist in database '{database_name}' of bucket '{bucket_name}'.")
        raise HTTPException(status_code=404, detail=f"Class '{class_name}' does not exist in database '{database_name}' of bucket '{bucket_name}'.")
    except Exception as e:
        logger.error(f"Failed to check if class '{class_name}' exists in database '{database_name}' of bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to check class existence: {str(e)}")
    
async def check_sample_exists(bucket_name: str, database_name: str, class_name: str, sample_name: str):
    """
    Checks if a specific sample (file) exists in a class (subfolder) within a database in the S3 bucket.

    :param bucket_name: The name of the S3 bucket.
    :param database_name: The name of the database.
    :param class_name: The name of the class (subfolder).
    :param sample_name: The name of the sample (file) to check.
    :raises HTTPException: If the sample does not exist.
    """
    if not database_name.endswith('/'):
        database_name += '/'
    if not class_name.endswith('/'):
        class_name += '/'
    sample_path = f"{database_name}{class_name}{sample_name}"

    try:
        objects = client.list_objects(bucket_name, prefix=sample_path, recursive=False)
        first_object = next(objects, None)
        if not first_object or first_object.object_name != sample_path:
            logger.error(f"Sample '{sample_name}' does not exist in class '{class_name}' of database '{database_name}' in bucket '{bucket_name}'.")
            raise HTTPException(status_code=404, detail=f"Sample '{sample_name}' does not exist in class '{class_name}' of database '{database_name}' in bucket '{bucket_name}'.")
    except StopIteration:
        logger.error(f"Sample '{sample_name}' does not exist in class '{class_name}' of database '{database_name}' in bucket '{bucket_name}'.")
        raise HTTPException(status_code=404, detail=f"Sample '{sample_name}' does not exist in class '{class_name}' of database '{database_name}' in bucket '{bucket_name}'.")
    except Exception as e:
        logger.error(f"Failed to check if sample '{sample_name}' exists in class '{class_name}' of database '{database_name}' in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to check sample existence: {str(e)}")