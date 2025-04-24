import logging
import os
from pathlib import Path
from fastapi import File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from minio import S3Error
from ..utils.minio_client import get_minio_client
from ..utils.minio_validators import check_bucket_exists, check_class_exists, check_database_exists, check_sample_exists_for_classification, check_sample_exists_for_regression_detection


client = get_minio_client()
logger = logging.getLogger(__name__)

async def upload_sample(bucket_name: str, database_name: str, class_name: str, 
                        file: UploadFile = File(...)):
    """
    Uploads a sample file with a .bmp extension to a specified class within a database in an S3 bucket.

    :param bucket_name: The name of the S3 bucket.
    :param database_name: The name of the database containing the class.
    :param class_name: The name of the class (subdirectory) to receive the file.
    :param file: The sample file to upload.
    :return: A message indicating the status of the upload.
    """
    file_extension = os.path.splitext(file.filename)[1]
    if file_extension != '.bmp':
        raise HTTPException(status_code=400, detail="Only .bmp files are accepted.")

    await check_bucket_exists(client, bucket_name)
    await check_database_exists(client, bucket_name, database_name)
    await check_class_exists(client, bucket_name, database_name, class_name)

    file_path = f"{database_name}/{class_name}/{file.filename}"
    
    try:
        # Seek to the end of the file to determine the size, then rewind
        file.file.seek(0, os.SEEK_END)
        file_size = file.file.tell()
        file.file.seek(0, os.SEEK_SET)

        client.put_object(
            bucket_name, file_path, file.file, file_size
        )
        logger.info(f"File '{file.filename}' uploaded successfully to '{file_path}' in bucket '{bucket_name}'.")
        return {"message": f"File '{file.filename}' uploaded successfully to '{file_path}'."}

    except S3Error as e:
        logger.error(f"Failed to upload file '{file.filename}' to '{file_path}' in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error occurred during file upload: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")
    
async def get_all_samples_for_classification(bucket_name: str, database_name: str, class_name: str):
    """
    Retrieves all samples (files) from a specified class within a database in an S3 bucket.

    :param bucket_name: The name of the S3 bucket.
    :param database_name: The name of the database within the bucket.
    :param class_name: The name of the class (subfolder) within the database.
    :return: A list of file names (objects) within the specified class.
    :raises HTTPException: If there is an error in fetching the samples.
    """
    await check_bucket_exists(client, bucket_name)
    await check_database_exists(client, bucket_name, database_name)
    await check_class_exists(client, bucket_name, database_name, class_name)

    if not database_name.endswith('/'):
        database_name += '/'
    if not class_name.endswith('/'):
        class_name += '/'
    full_prefix = f"{database_name}{class_name}"

    try:
        objects = client.list_objects(bucket_name, prefix=full_prefix, recursive=True)
        # Create a list to store the names of the objects, stripping the prefix from each name
        sample_list = [obj.object_name.replace(full_prefix, "") for obj in objects if obj.object_name != full_prefix and not obj.is_dir]
        return {"samples": sample_list}
    except Exception as e:
        logger.error(f"Failed to retrieve samples from '{class_name}' in '{database_name}' of bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve samples: {str(e)}")
    
async def get_all_samples_for_regression_detection(bucket_name: str, database_name: str):
    """
    Retrieves all samples (files) from a specified database in an S3 bucket for regression or detection.

    :param bucket_name: The name of the S3 bucket.
    :param database_name: The name of the database within the bucket.
    :return: A list of file names (objects) within the specified database.
    :raises HTTPException: If there is an error in fetching the samples.
    """
    await check_bucket_exists(client, bucket_name)
    await check_database_exists(client, bucket_name, database_name)

    if not database_name.endswith('/'):
        database_name += '/'
    full_prefix = database_name

    try:
        objects = client.list_objects(bucket_name, prefix=full_prefix, recursive=True)
        # Create a list to store the names of the objects, stripping the prefix from each name
        sample_list = [obj.object_name.replace(full_prefix, "") for obj in objects if obj.object_name != full_prefix and not obj.is_dir]
        return {"samples": sample_list}
    except Exception as e:
        logger.error(f"Failed to retrieve samples from '{database_name}' of bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve samples: {str(e)}")
    
async def download_sample(bucket_name: str, database_name: str, sample_name: str, class_name: str = None):
    """
    Downloads a specific sample (file) from a class within a database in the S3 bucket,
    and returns the file for download.

    :param bucket_name: The name of the S3 bucket where the database is stored.
    :param database_name: The name of the database (prefix) inside the bucket.
    :param class_name: The name of the class (subdirectory) inside the database.
    :param sample_name: The name of the sample (file) to download.
    :return: A `FileResponse` object containing the downloaded file.
    """
    try:
        await check_bucket_exists(client, bucket_name)
        await check_database_exists(client, bucket_name, database_name)

        if class_name:
            await check_sample_exists_for_classification(bucket_name, database_name, class_name, sample_name)
            file_path = f"{database_name}/{class_name}/{sample_name}"
        else:
            await check_sample_exists_for_regression_detection(bucket_name, database_name, sample_name)
            file_path = f"{database_name}/{sample_name}"

        # Path to the system's Downloads folder
        downloads_path = str(Path.home() / "Downloads")
        sample_download_path = os.path.join(downloads_path, sample_name)

        # Download the sample file
        client.fget_object(bucket_name, file_path, sample_download_path)

        # Return the sample file as a downloadable file
        return FileResponse(path=sample_download_path, filename=sample_name, media_type='application/octet-stream')
    
    except S3Error as e:
        logger.error(f"Failed to download sample '{sample_name}' from class '{class_name}' in database '{database_name}' in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to download sample: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error occurred while downloading sample '{sample_name}' from class '{class_name}' in database '{database_name}' in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")
    
async def delete_sample(bucket_name: str, database_name: str, sample_name: str, class_name: str = None):
    """
    Deletes a specific sample (file) within a class or directly within a dataset from an S3 bucket.

    :param bucket_name: The name of the S3 bucket.
    :param database_name: The name of the database containing the class or dataset.
    :param sample_name: The name of the sample (file) to delete.
    :param class_name: The name of the class (subdirectory) containing the sample. Optional for regression/detection.
    :return: A message indicating the status of the deletion.
    """
    try:
        await check_bucket_exists(client, bucket_name)
        await check_database_exists(client, bucket_name, database_name)
        
        if class_name:
            await check_sample_exists_for_classification(bucket_name, database_name, class_name, sample_name)
            file_path = f"{database_name}/{class_name}/{sample_name}"
        else:
            await check_sample_exists_for_regression_detection(bucket_name, database_name, sample_name)
            file_path = f"{database_name}/{sample_name}"

        client.remove_object(bucket_name, file_path)

        logger.info(f"Sample '{sample_name}' deleted successfully from {'class ' + class_name if class_name else 'dataset'} in bucket '{bucket_name}'.")
        return {"message": f"Sample '{sample_name}' deleted successfully from {'class ' + class_name if class_name else 'dataset'} in database '{database_name}' in bucket '{bucket_name}'."}

    except S3Error as e:
        logger.error(f"Failed to delete sample '{sample_name}' from {'class ' + class_name if class_name else 'dataset'} in database '{database_name}' in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete sample: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error occurred while deleting sample '{sample_name}' from {'class ' + class_name if class_name else 'dataset'} in database '{database_name}' in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")