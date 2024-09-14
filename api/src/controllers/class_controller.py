import os
import logging
from fastapi import HTTPException
from ..utils.minio_client import get_minio_client
from ..utils.minio_validators import check_bucket_exists, check_class_exists, check_directory_exists
from minio.error import S3Error

client = get_minio_client()
logger = logging.getLogger(__name__)

async def download_class(bucket_name: str, directory_name: str, class_name: str):
    """
    Downloads all files from a specified class (subdirectory) within a directory in the S3 bucket to a local directory.
    
    :param bucket_name: The name of the S3 bucket.
    :param directory_name: The name of the directory inside the bucket.
    :param class_name: The name of the class (subdirectory) inside the directory.
    :return: A message indicating the status of the download.
    """
    try:
        await check_bucket_exists(bucket_name)
        await check_directory_exists(bucket_name, directory_name)
        await check_class_exists(bucket_name, directory_name, class_name)

        class_path = f"{directory_name}/{class_name}/"
        objects = client.list_objects(bucket_name, prefix=class_path, recursive=True)
        dir_path = "/tmp"
        class_download_path = os.path.join(dir_path, class_name)

        if not os.path.exists(class_download_path):
            os.makedirs(class_download_path)

        for obj in objects:
            if not obj.is_dir:
                relative_path = obj.object_name[len(class_path):]
                file_path = os.path.join(class_download_path, relative_path)
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                client.fget_object(bucket_name, obj.object_name, file_path)
                logger.info(f"File '{obj.object_name}' downloaded successfully to '{file_path}'.")

        return {"message": f"All files from class '{class_name}' in directory '{directory_name}' downloaded successfully.", "directory_location": class_download_path}
    
    except S3Error as e:
        logger.error(f"Failed to download class '{class_name}' from directory '{directory_name}' in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to download class: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error occurred while downloading class '{class_name}' from directory '{directory_name}' in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

async def delete_class(bucket_name: str, directory_name: str, class_name: str):
    """
    Deletes a specific class (subdirectory) within a directory from the S3 bucket.

    :param bucket_name: The name of the S3 bucket.
    :param directory_name: The name of the directory containing the class.
    :param class_name: The name of the class (subdirectory) to delete.
    :return: A message indicating the status of the deletion.
    """
    try:
        await check_bucket_exists(bucket_name)
        await check_directory_exists(bucket_name, directory_name)
        await check_class_exists(bucket_name, directory_name, class_name)

        class_path = f"{directory_name}/{class_name}/"
        objects = client.list_objects(bucket_name, prefix=class_path, recursive=True)
        for obj in objects:
            client.remove_object(bucket_name, obj.object_name)
            logger.info(f"Object '{obj.object_name}' deleted successfully from class '{class_name}' in bucket '{bucket_name}'.")

        return {"message": f"Class '{class_name}' deleted successfully from directory '{directory_name}' in bucket '{bucket_name}'."}

    except S3Error as e:
        logger.error(f"Failed to delete class '{class_name}' from directory '{directory_name}' in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete class: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error occurred while deleting class '{class_name}' from directory '{directory_name}' in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")