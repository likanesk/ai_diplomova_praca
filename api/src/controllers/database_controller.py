import os
import tempfile
import logging
import zipfile
from fastapi import HTTPException, File, UploadFile
from ..utils.minio_client import get_minio_client
from ..utils.minio_validators import check_bucket_exists, check_database_exists
from minio.error import S3Error

client = get_minio_client()
logger = logging.getLogger(__name__)

async def validate_zip_structure(temp_dir: str, expected_num_classes: int, expected_num_files_per_class: int) -> bool:
    # Track the number of class folders
    class_folders = []

    # Walk through the extracted files to validate structure
    db_folder = None
    for root, dirs, files in os.walk(temp_dir):
        rel_path = os.path.relpath(root, temp_dir).replace('\\', '/')

        # Check top-level folder (<DB>)
        if rel_path == '.':
            if len(dirs) != 1:
                raise HTTPException(status_code=400, detail="There should be exactly one top-level <DB> folder.")
            db_folder = dirs[0]

        # Validate classes inside DB folder
        elif rel_path.count('/') == 0 and root.split(os.sep)[-1] == db_folder:
            if not dirs:
                raise HTTPException(status_code=400, detail="The <DB> folder must contain class folders.")
            class_folders.extend(dirs)
        
        # Validate that class folders contain only image files and no additional subfolders
        elif rel_path.count('/') == 1:  # This assumes we're inside a class folder now
            if dirs:
                raise HTTPException(status_code=400, detail=f"Class folder '{root}' contains subfolders, which is not allowed.")
            if len(files) != expected_num_files_per_class:
                raise HTTPException(status_code=400, detail=f"Class folder '{root}' contains {len(files)} files, but {expected_num_files_per_class} were expected.")
            for file in files:
                if not file.lower().endswith(('.bmp', '.jpg', '.jpeg', '.png', '.gif')):  # Check image formats
                    raise HTTPException(status_code=400, detail=f"File '{file}' in class folder '{root}' is not a valid image.")

    # Check the number of class folders
    if len(class_folders) != expected_num_classes:
        raise HTTPException(status_code=400, detail=f"Expected {expected_num_classes} class folders, but found {len(class_folders)}.")

    return True

async def upload_zip(bucket_name: str, file: UploadFile = File(...), expected_num_classes: int = 4, expected_num_files_per_class: int = 200):
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

            # Validate the folder structure and number of classes/files before uploading
            await validate_zip_structure(temp_dir, expected_num_classes, expected_num_files_per_class)

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

    return {"message": "Database uploaded successfully"}

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
    Deletes an entire database from the S3 bucket.

    :param bucket_name: The name of the S3 bucket.
    :param database_name: The name of the database to delete.
    :return: A message indicating the status of the deletion.
    """
    try:
        await check_bucket_exists(bucket_name)
        await check_database_exists(bucket_name, database_name)

        objects = client.list_objects(bucket_name, prefix=database_name, recursive=True)
        for obj in objects:
            client.remove_object(bucket_name, obj.object_name)
            logger.info(f"Object '{obj.object_name}' deleted successfully from bucket '{bucket_name}'.")

        return {"message": f"Database '{database_name}' deleted successfully from bucket '{bucket_name}'."}

    except S3Error as e:
        logger.error(f"Failed to delete database '{database_name}' from bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete database: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error occurred while deleting database '{database_name}' from bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")