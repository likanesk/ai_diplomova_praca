import os
import re
import tempfile
import logging
import zipfile
import json
from fastapi import FastAPI, HTTPException, File, Query, UploadFile
from ..utils.minio_client import get_minio_client
from ..utils.minio_validators import check_bucket_exists, check_database_exists
from minio.error import S3Error

app = FastAPI()
client = get_minio_client()
logger = logging.getLogger(__name__)

VALID_EXTENSIONS = {'.bmp', '.jpg', '.jpeg', '.png', '.gif'}

def is_valid_image(file_name: str) -> bool:
    _, ext = os.path.splitext(file_name)
    return ext in VALID_EXTENSIONS

def validate_class_folder(class_folder: str, files: list, expected_num_files_per_class: int) -> None:
    if not files:
        raise HTTPException(status_code=400, detail=f"Class folder '{class_folder}' contains no files.")

    file_numbers = set(range(1, expected_num_files_per_class + 1))
    found_files = set()

    for file in files:
        base_name, ext = os.path.splitext(file)
        if is_valid_image(file):
            try:
                file_num = int(base_name)
                if 1 <= file_num <= expected_num_files_per_class:
                    found_files.add(file_num)
            except ValueError:
                continue  # Skip files that cannot be converted to integer
        else:
            raise HTTPException(status_code=400, detail=f"File '{file}' in class folder '{class_folder}' is not a valid image. Accepted file formats are: {', '.join(VALID_EXTENSIONS)}.")

    missing_numbers = file_numbers - found_files
    if missing_numbers:
        raise HTTPException(status_code=400, detail=f"Missing files for expected numbers: {sorted(missing_numbers)} in class folder '{class_folder}'.")

    if len(files) != expected_num_files_per_class:
        raise HTTPException(status_code=400, detail=f"Class folder '{class_folder}' contains {len(files)} files, but {expected_num_files_per_class} were expected.")

async def validate_classification(temp_dir: str, expected_num_classes: int, expected_num_files_per_class: int) -> dict:
    errors = {"missing_classes": [], "missing_files": [], "invalid_structure": []}
    class_folders = []
    db_folder = None

    for current_folder, subfolders, files_in_current_folder in os.walk(temp_dir):
        rel_path = os.path.relpath(current_folder, temp_dir).replace('\\', '/')

        # Validate top-level folder (DB folder)
        if rel_path == '.':
            if len(subfolders) != 1:
                errors["invalid_structure"].append("There should be exactly one top-level <DB> folder.")
                return errors
            db_folder = subfolders[0]

        elif rel_path.count('/') == 0 and os.path.basename(current_folder) == db_folder:
            if not subfolders:
                errors["invalid_structure"].append("The <DB> folder must contain class folders.")
                return errors
            class_folders.extend(subfolders)

        # Validate individual class folders and their contents
        elif rel_path.count('/') == 1:
            if subfolders:
                errors["invalid_structure"].append(f"Class folder '{current_folder}' contains subfolders, which is not allowed.")
                return errors
            
            try:
                validate_class_folder(current_folder, files_in_current_folder, expected_num_files_per_class)
            except HTTPException as e:
                errors["missing_files"].append(e.detail)

    # Final check to ensure the correct number of class folders
    if len(class_folders) != expected_num_classes:
        errors["missing_classes"].append(f"Expected {expected_num_classes} class folders, but found {len(class_folders)}.")

    return errors

def validate_regression_data(data: list) -> dict:
    errors = {"missing_images": [], "invalid_regression": []}
    for item in data:
        if "file" not in item or "regression" not in item:
            raise HTTPException(status_code=400, detail="Invalid JSON structure. Each item must contain 'file' and 'regression' keys.")
        
        # Check the regression value
        if not isinstance(item["regression"], (float, int)):
            errors["invalid_regression"].append(item["file"])
    return errors

def validate_detection_data(data: list) -> dict:
    errors = {"missing_images": [], "invalid_roi": []}
    for item in data:
        if "file" not in item or "ROI" not in item:
            raise HTTPException(status_code=400, detail="Invalid JSON structure. Each item must contain 'file' and 'ROI' keys.")
        
        # Check the ROI and ROI values
        roi = item["ROI"]
        if not isinstance(roi, list) or len(roi) != 4:
            errors["invalid_roi"].append(item["file"])
        else:
            L, T, R, B = roi
            if not (isinstance(L, int) and isinstance(T, int) and isinstance(R, int) and isinstance(B, int)):
                errors["invalid_roi"].append(item["file"])
            if not (L < R and T < B):
                errors["invalid_roi"].append(item["file"])
    return errors

def validate_zip_structure(temp_dir: str) -> dict:
    """
    Validation of folder structure for detection and regression.
    It expects the exact structure: /DB/samples+json.
    """
    errors = {"invalid_structure": []}

    db_folder = None

    for current_folder, subfolders, files_in_current_folder in os.walk(temp_dir):
        rel_path = os.path.relpath(current_folder, temp_dir).replace('\\', '/')

        # Validate top-level folder (DB folder)
        if rel_path == '.':
            if len(subfolders) != 1:
                errors["invalid_structure"].append("There should be exactly one top-level <DB> folder.")
                return errors
            db_folder = subfolders[0]
            
    db_folder_path = os.path.join(temp_dir, db_folder)

    # Check that the DB folder contains only files (no other folders)
    for item in os.listdir(db_folder_path):
        item_path = os.path.join(db_folder_path, item)
        if os.path.isdir(item_path):
            errors["invalid_structure"].append(f"The '{db_folder}' folder should not contain any subfolders.")
            return errors

    return errors

async def validate_zip_contents(temp_dir: str, is_regression: bool) -> dict:
    """
    Validation of ZIP file contents for regression or detection.
    It expects the structure: /<DB>/images+json.
    """
    errors = {"missing_images": [], "missing_annotations": [], "invalid_data": [], "invalid_structure": []}

    # Validate folder structure
    structure_errors = validate_zip_structure(temp_dir)
    if structure_errors["invalid_structure"]:
        errors.update(structure_errors)
        return errors

    db_folder = os.listdir(temp_dir)[0]
    db_folder_path = os.path.join(temp_dir, db_folder)

    # Validate JSON file
    json_files = [f for f in os.listdir(db_folder_path) if f.endswith('.json')]
    if not json_files:
        errors["invalid_structure"].append("JSON file not found in the DB folder.")
        return errors
    
    json_file_path = os.path.join(db_folder_path, json_files[0])
    with open(json_file_path, 'r') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            errors["invalid_structure"].append("Invalid JSON file.")
            return errors

    # Validate data by type
    if is_regression:
        errors.update(validate_regression_data(data))
    else:
        errors.update(validate_detection_data(data))

    # Check images and annotations
    image_files = [f for f in os.listdir(db_folder_path) if is_valid_image(f)]
    image_names_in_json = [item["file"] for item in data if "file" in item]

    # 1. Images included in JSON, but not in ZIP
    for image_name in image_names_in_json:
        if image_name not in image_files:
            errors["missing_images"].append(image_name)

    # 2. Images included in ZIP, but not in JSON
    for image_name in image_files:
        if image_name not in image_names_in_json:
            errors["missing_annotations"].append(image_name)

    return errors

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

async def upload_classification(
    bucket_name: str,
    expected_num_classes: int = Query(..., description="Expected number of classes"),
    expected_num_files_per_class: int = Query(..., description="Expected number of files per class"),
    file: UploadFile = File(..., description="ZIP file to upload")
):
    return await process_zip(
        file,
        bucket_name,
        validate_classification,
        expected_num_classes=expected_num_classes,
        expected_num_files_per_class=expected_num_files_per_class
    )

async def upload_regression(
    bucket_name: str,
    file: UploadFile = File(..., description="ZIP file to upload")
):
    return await process_zip(file, bucket_name, validate_zip_contents, is_regression=True)

async def upload_detection(
    bucket_name: str,
    file: UploadFile = File(..., description="ZIP file to upload")
):
    return await process_zip(file, bucket_name, validate_zip_contents, is_regression=False)

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