import os
import tempfile
import logging
import zipfile
from fastapi import HTTPException, File, UploadFile
from ..utils.minio_client import get_minio_client
from ..utils.minio_validators import check_bucket_exists, check_directory_exists
from minio.error import S3Error

client = get_minio_client()
logger = logging.getLogger(__name__)

async def upload_zip(bucket_name: str, file: UploadFile = File(...)):
    if not file.filename.endswith('.zip'):
        raise HTTPException(status_code=400, detail="File is not a zip.")
    
    await check_bucket_exists(bucket_name)

    # Use a temporary directory for extraction and processing
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_file_path = os.path.join(temp_dir, file.filename)
        with open(temp_file_path, 'wb+') as f:
            f.write(await file.read())

        # Unzip the file within the temporary directory
        with zipfile.ZipFile(temp_file_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

            # Walk through the directory structure and upload each file
            for root, dirs, files in os.walk(temp_dir):
                for filename in files:
                    if root != temp_dir:
                        file_path = os.path.join(root, filename)
                        object_name = os.path.relpath(file_path, temp_dir).replace('\\', '/')

                        with open(file_path, "rb") as data:
                            try:
                                client.put_object(
                                    bucket_name,
                                    object_name,
                                    data,
                                    os.path.getsize(file_path)
                                )
                            except S3Error as e:
                                raise HTTPException(status_code=500, detail=f"Failed to upload {object_name}: {str(e)}")

    return {"message": "Directory uploaded successfully"}

async def download_directory(bucket_name: str, directory_name: str):
    """
    Downloads all files from a specified directory in the S3 bucket to a local directory.
    
    :param bucket_name: The name of the S3 bucket.
    :param directory_name: The name of the directory inside the bucket.
    :return: A message indicating the status of the download.
    """
    try:
        await check_bucket_exists(bucket_name)

        objects = client.list_objects(bucket_name, prefix=directory_name, recursive=True)
        dir_path = "/tmp"

        for obj in objects:
            file_path = f"{dir_path}/{obj.object_name}"
            client.fget_object(bucket_name, obj.object_name, file_path)
            logger.info(f"File '{obj.object_name}' downloaded successfully to '{file_path}'.")

        return {"message": f"All files from directory '{directory_name}' downloaded successfully.", "directory_location": dir_path}
    
    except S3Error as e:
        logger.error(f"Failed to download directory '{directory_name}' from bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to download directory: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error occurred while downloading directory '{directory_name}' from bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

async def delete_directory(bucket_name: str, directory_name: str):
    """
    Deletes an entire directory from the S3 bucket.

    :param bucket_name: The name of the S3 bucket.
    :param directory_name: The name of the directory to delete.
    :return: A message indicating the status of the deletion.
    """
    try:
        await check_bucket_exists(bucket_name)
        await check_directory_exists(bucket_name, directory_name)

        objects = client.list_objects(bucket_name, prefix=directory_name, recursive=True)
        for obj in objects:
            client.remove_object(bucket_name, obj.object_name)
            logger.info(f"Object '{obj.object_name}' deleted successfully from bucket '{bucket_name}'.")

        return {"message": f"Directory '{directory_name}' deleted successfully from bucket '{bucket_name}'."}

    except S3Error as e:
        logger.error(f"Failed to delete directory '{directory_name}' from bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete directory: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error occurred while deleting directory '{directory_name}' from bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")