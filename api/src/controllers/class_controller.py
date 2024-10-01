import os
import logging
import tempfile
import zipfile
from fastapi import File, HTTPException, UploadFile
from ..utils.minio_client import get_minio_client
from ..utils.minio_validators import check_bucket_exists, check_class_exists, check_database_exists
from minio.error import S3Error

client = get_minio_client()
logger = logging.getLogger(__name__)

async def upload_class(bucket_name: str, database_name: str, file: UploadFile = File(...)):
    """
    Uploads a class (a structured set of files and databases from a zip file) to a specified database within an S3 bucket.

    :param bucket_name: The name of the S3 bucket.
    :param database_name: The database within the bucket to place the class.
    :param file: The zip file containing the class data.
    :return: A message indicating the status of the upload.
    """
    if not file.filename.endswith('.zip'):
        raise HTTPException(status_code=400, detail="Only zip files are accepted.")
    
    await check_bucket_exists(bucket_name)

    # Use a temporary database for extraction and processing
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_file_path = os.path.join(temp_dir, file.filename)
        with open(temp_file_path, 'wb+') as f:
            f.write(await file.read())

        # Unzip the file within the temporary database
        with zipfile.ZipFile(temp_file_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

            # Ensure to skip uploading the .zip file itself, upload only extracted contents
            for root, dirs, files in os.walk(temp_dir):
                # Skip the database where the zip itself was saved
                if root == temp_dir:
                    continue
                for filename in files:
                    file_path = os.path.join(root, filename)
                    object_name = os.path.join(database_name, os.path.relpath(file_path, temp_dir)).replace('\\', '/')
                    
                    with open(file_path, "rb") as data:
                        try:
                            client.put_object(
                                bucket_name,
                                object_name,
                                data,
                                os.path.getsize(file_path)
                            )
                            logger.info(f"File '{object_name}' uploaded successfully to bucket '{bucket_name}'.")
                        except S3Error as e:
                            logger.error(f"Failed to upload {object_name}: {str(e)}")
                            raise HTTPException(status_code=500, detail=f"Failed to upload {object_name}: {str(e)}")

    return {"message": f"Class data uploaded successfully to '{database_name}' in bucket '{bucket_name}'."}
    
async def get_all_classes(bucket_name: str, database_name: str):
    """
    Retrieves all classes (subfolders) from a specified database within an S3 bucket.

    :param bucket_name: The name of the S3 bucket.
    :param database_name: The name of the database within the bucket.
    :return: A list of class names (subfolders) within the specified database.
    :raises HTTPException: If there is an error in fetching the classes.
    """
    await check_bucket_exists(bucket_name)
    await check_database_exists(bucket_name, database_name)

    if not database_name.endswith('/'):
        database_name += '/'

    try:
        objects = client.list_objects(bucket_name, prefix=database_name, recursive=False)
        # Filter to get only databases (objects ending with '/')
        class_list = [obj.object_name[len(database_name):-1] for obj in objects if obj.object_name.endswith('/')]
        return {"classes": class_list}
    except Exception as e:
        logger.error(f"Failed to retrieve classes in database '{database_name}' of bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve classes: {str(e)}")

async def download_class(bucket_name: str, database_name: str, class_name: str):
    """
    Downloads all files from a specified class (subdirectory) within a database in the S3 bucket to a local database.
    
    :param bucket_name: The name of the S3 bucket.
    :param database_name: The name of the database inside the bucket.
    :param class_name: The name of the class (subdirectory) inside the database.
    :return: A message indicating the status of the download.
    """
    try:
        await check_bucket_exists(bucket_name)
        await check_database_exists(bucket_name, database_name)
        await check_class_exists(bucket_name, database_name, class_name)

        class_path = f"{database_name}/{class_name}/"
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

        return {"message": f"All files from class '{class_name}' in database '{database_name}' downloaded successfully.", "database_location": class_download_path}
    
    except S3Error as e:
        logger.error(f"Failed to download class '{class_name}' from database '{database_name}' in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to download class: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error occurred while downloading class '{class_name}' from database '{database_name}' in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

async def delete_class(bucket_name: str, database_name: str, class_name: str):
    """
    Deletes a specific class (subdirectory) within a database from the S3 bucket.

    :param bucket_name: The name of the S3 bucket.
    :param database_name: The name of the database containing the class.
    :param class_name: The name of the class (subdirectory) to delete.
    :return: A message indicating the status of the deletion.
    """
    try:
        await check_bucket_exists(bucket_name)
        await check_database_exists(bucket_name, database_name)
        await check_class_exists(bucket_name, database_name, class_name)

        class_path = f"{database_name}/{class_name}/"
        objects = client.list_objects(bucket_name, prefix=class_path, recursive=True)
        for obj in objects:
            client.remove_object(bucket_name, obj.object_name)
            logger.info(f"Object '{obj.object_name}' deleted successfully from class '{class_name}' in bucket '{bucket_name}'.")

        return {"message": f"Class '{class_name}' deleted successfully from database '{database_name}' in bucket '{bucket_name}'."}

    except S3Error as e:
        logger.error(f"Failed to delete class '{class_name}' from database '{database_name}' in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete class: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error occurred while deleting class '{class_name}' from database '{database_name}' in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")