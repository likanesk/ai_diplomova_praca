import os
import re
import tempfile
import logging
import zipfile
from fastapi import FastAPI, HTTPException, File, Query, UploadFile
from ..validators.classification_validator import ClassificationValidator
from ..validators.detection_validator import DetectionValidator
from ..validators.regression_validator import RegressionValidator
from ..utils.minio_client import get_minio_client
from ..utils.minio_validators import check_bucket_exists, check_database_exists
from minio.error import S3Error

app = FastAPI()
client = get_minio_client()
logger = logging.getLogger(__name__)

VALID_EXTENSIONS = {'.bmp', '.jpg', '.jpeg', '.png', '.gif'}

# Initialization of validators
classification_validator = ClassificationValidator(VALID_EXTENSIONS)
regression_validator = RegressionValidator(VALID_EXTENSIONS)
detection_validator = DetectionValidator(VALID_EXTENSIONS)

async def upload_classification(
    bucket_name: str,
    expected_num_classes: int = Query(...),
    expected_num_files_per_class: int = Query(...),
    file: UploadFile = File(...)
):
    return await process_zip(
        file,
        bucket_name,
        classification_validator.validate_classification,
        expected_num_classes=expected_num_classes,
        expected_num_files_per_class=expected_num_files_per_class
    )

async def upload_regression(bucket_name: str, file: UploadFile = File(...)):
    return await process_zip(file, bucket_name, regression_validator.validate)

async def upload_detection(bucket_name: str, file: UploadFile = File(...)):
    return await process_zip(file, bucket_name, detection_validator.validate)

async def process_zip(file: UploadFile, bucket_name: str, validation_func, **kwargs):
    """
    Common function for ZIP file processing.
    It only uploads the extracted contents of the ZIP file, without the .zip file itself.
    """
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

        # Validation of ZIP contents
        errors = await validation_func(temp_dir, **kwargs)
        if any(errors.values()):
            raise HTTPException(
                status_code=400,
                detail={
                    "message": "Validation failed for the ZIP contents.",
                    "errors": errors
                }
            )

        # Upload into MinIO (ignore original ZIP file)
        db_folder = os.listdir(temp_dir)[0]
        db_folder_path = os.path.join(temp_dir, db_folder)

        for root, _, files in os.walk(db_folder_path):
            for filename in files:
                file_path = os.path.join(root, filename)
                # Ignore the original ZIP file
                if file_path == temp_file_path:
                    continue

                # Create an object name without a temporary directory
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

    return {"message": "ZIP file uploaded and validated successfully."}

async def get_all_databases(bucket_name: str):
    """
    Retrieves all top-level databases from a specified S3 bucket.

    :param bucket_name: The name of the S3 bucket.
    :return: A list of top-level database names within the specified bucket.
    :raises HTTPException: If there is an error in fetching the databases.
    """
    await check_bucket_exists(bucket_name)

    try:
        objects = client.list_objects(bucket_name)
        # Filter to get only databases (objects with names ending in '/')
        database_list = [obj.object_name.rstrip('/') for obj in objects if obj.object_name.endswith('/')]
        return {"databases": database_list}
    except Exception as e:
        logger.error(f"Failed to retrieve databases in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve databases: {str(e)}")

async def download_database(bucket_name: str, database_name: str):
    """
    Downloads all files from a specified database in the S3 bucket to a local database.
    
    :param bucket_name: The name of the S3 bucket.
    :param database_name: The name of the database inside the bucket.
    :return: A message indicating the status of the download.
    """
    try:
        await check_bucket_exists(bucket_name)

        objects = client.list_objects(bucket_name, prefix=database_name, recursive=True)
        dir_path = "/tmp"

        for obj in objects:
            file_path = f"{dir_path}/{obj.object_name}"
            client.fget_object(bucket_name, obj.object_name, file_path)
            logger.info(f"File '{obj.object_name}' downloaded successfully to '{file_path}'.")

        return {"message": f"All files from database '{database_name}' downloaded successfully.", "database_location": dir_path}
    
    except S3Error as e:
        logger.error(f"Failed to download database '{database_name}' from bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to download database: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error occurred while downloading database '{database_name}' from bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

async def delete_database(bucket_name: str, database_name: str):
    """
    Deletes an entire database from the S3 bucket based on exact name match.

    :param bucket_name: The name of the S3 bucket.
    :param database_name: The name of the database to delete.
    :return: A message indicating the status of the deletion.
    """
    try:
        await check_bucket_exists(bucket_name)
        await check_database_exists(bucket_name, database_name)

        objects = client.list_objects(bucket_name, recursive=True)

        database_prefix = f"{database_name}/"
        database_objects = [
            obj for obj in objects
            if obj.object_name.startswith(database_prefix)
            or obj.object_name == database_name
        ]

        if not database_objects:
            logger.warning(f"No objects found for database '{database_name}' in bucket '{bucket_name}'.")
            return {"message": f"No objects found for database '{database_name}' in bucket '{bucket_name}'."}

        for obj in database_objects:
            client.remove_object(bucket_name, obj.object_name)
            logger.info(f"Object '{obj.object_name}' deleted successfully from bucket '{bucket_name}'.")

        return {"message": f"Database '{database_name}' deleted successfully from bucket '{bucket_name}'."}

    except S3Error as e:
        logger.error(f"Failed to delete database '{database_name}' from bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete database: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error occurred while deleting database '{database_name}' from bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")