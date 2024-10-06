import os
import re
import tempfile
import logging
import zipfile
from fastapi import HTTPException, File, UploadFile
from ..utils.minio_client import get_minio_client
from ..utils.minio_validators import check_bucket_exists, check_database_exists
from minio.error import S3Error

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

async def validate_zip_structure(temp_dir: str, expected_num_classes: int, expected_num_files_per_class: int) -> bool:
    class_folders = []
    db_folder = None

    for current_folder, subfolders, files_in_current_folder in os.walk(temp_dir):
        rel_path = os.path.relpath(current_folder, temp_dir).replace('\\', '/')

        # Validate top-level folder (DB folder)
        if rel_path == '.':
            if len(subfolders) != 1:
                raise HTTPException(status_code=400, detail="There should be exactly one top-level <DB> folder.")
            db_folder = subfolders[0]

        elif rel_path.count('/') == 0 and os.path.basename(current_folder) == db_folder:
            if not subfolders:
                raise HTTPException(status_code=400, detail="The <DB> folder must contain class folders.")
            class_folders.extend(subfolders)

        # Validate individual class folders and their contents
        elif rel_path.count('/') == 1:
            if subfolders:
                raise HTTPException(status_code=400, detail=f"Class folder '{current_folder}' contains subfolders, which is not allowed.")
            
            validate_class_folder(current_folder, files_in_current_folder, expected_num_files_per_class)

    # Final check to ensure the correct number of class folders
    if len(class_folders) != expected_num_classes:
        raise HTTPException(status_code=400, detail=f"Expected {expected_num_classes} class folders, but found {len(class_folders)}.")

    return True

# Regex patterns for filename validation
pattern_with_class = re.compile(r'^[A-Z0-9]{1,6}[\s\-_]*[\s\-_]+[\s\-_]*[0-9]{2,4}$')  # Matches CIFF, CIFFF, CIFFFF also CIIIFF, CIIIFFF, CIIIFFFF
pattern_without_class = re.compile(r'^[0-9]{2,4}$')  # Matches FFFF, FFF, FF

async def validate_flat_zip_structure(temp_dir: str, expected_num_classes: int, expected_num_files_per_class: int) -> bool:
    # Track the number of class folders and filenames
    file_count_per_class = {}
    files_outside_classes = []  # Initialize list to store files outside class folders

    # Ensure the folder for the ZIP file exists
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    # Walk through the extracted files
    for root, dirs, files in os.walk(temp_dir):
        rel_path = os.path.relpath(root, temp_dir).replace('\\', '/')

        # If we are at the top-level folder
        if rel_path == '.' or rel_path == os.path.basename(temp_dir):
            # If a folder matches the ZIP file name, process the files within it
            if temp_dir in dirs:
                # Process files inside this folder as if it was the top-level directory
                folder_path = os.path.join(root, temp_dir)
                for _, _, inner_files in os.walk(folder_path):
                    for file in inner_files:
                        file_name_without_ext, file_ext = os.path.splitext(file)

                        # Skip the file if it matches the ZIP file's base name
                        if file_name_without_ext == temp_dir:
                            continue  # Ignore this file completely

                        # Otherwise, add the file for processing
                        files_outside_classes.append(os.path.join(folder_path, file))
                dirs.remove(temp_dir)  # Remove this folder to avoid further walking

            # Process any files in the top-level directory
            for file in files:
                file_name_without_ext, file_ext = os.path.splitext(file)

                # Skip the file if it matches the ZIP file's base name
                if file_name_without_ext == temp_dir:
                    continue  # Ignore this file completely

                # Otherwise, add it to files_outside_classes for processing
                file_path = os.path.join(root, file)
                files_outside_classes.append(file_path)

        # Handle files inside valid class folders
        elif rel_path.count('/') == 1:  # We're inside a class folder now
            class_name = os.path.basename(root)
            if dirs:
                raise HTTPException(status_code=400, detail=f"Class folder '{class_name}' contains subfolders, which is not allowed.")

            if len(files) != expected_num_files_per_class:
                raise HTTPException(status_code=400, detail=f"Class folder '{class_name}' contains {len(files)} files, but {expected_num_files_per_class} were expected.")

            for file in files:
                file_name_without_ext, file_ext = os.path.splitext(file)

                # Skip the file if it matches the ZIP file's base name
                if file_name_without_ext == temp_dir:
                    continue  # Ignore this file completely

                # Validate the filename in the format CIFFFF/CIFFF/CIFF
                if pattern_with_class.match(file_name_without_ext):
                    # Extract class identifier and file part
                    file_class, _, file_part = file_name_without_ext.partition(' ' if ' ' in file_name_without_ext else '_')

                    # If the file class doesn't match the folder class, raise an error
                    if file_class != class_name:
                        raise HTTPException(status_code=400, detail=f"File '{file}' is in class '{class_name}', but its class part is '{file_class}'.")

                    # Rename the file to the FFFF/FFF/FF format (without the class part)
                    new_file_name = f"{file_part}{file_ext}"
                    new_file_path = os.path.join(root, new_file_name)
                    os.rename(os.path.join(root, file), new_file_path)

                elif not pattern_without_class.match(file_name_without_ext):
                    # If the file name doesn't match the expected pattern, raise an error
                    raise HTTPException(status_code=400, detail=f"File '{file}' in class folder '{class_name}' does not follow the expected format.")
                
            # Track the number of files per class
            file_count_per_class[class_name] = len(files)

    # Process files outside class folders (or in the folder named after the ZIP)
    if files_outside_classes:
        for file_path in files_outside_classes:
            file_name = os.path.basename(file_path)
            file_name_without_ext, file_ext = os.path.splitext(file_name)

            # Skip the file if it matches the ZIP file's base name
            if file_name_without_ext == temp_dir:
                continue  # Ignore this file completely

            # Validate the file format (CIFFFF/CIFFF/CIFF)
            if not pattern_with_class.match(file_name_without_ext):
                raise HTTPException(status_code=400, detail=f"File '{file_name}' does not follow the CIFFFF/CIFFF/CIFF format.")

            # Extract class identifier and file part
            file_class, _, file_part = file_name_without_ext.partition(' ' if ' ' in file_name_without_ext else '_')

            # Create class folder inside the folder named after the ZIP file
            class_folder = os.path.join(temp_dir, file_class)
            os.makedirs(class_folder, exist_ok=True)

            # Move and rename the file to FFFF/FFF/FF format in the class folder
            new_file_name = f"{file_part}{file_ext}"
            new_file_path = os.path.join(class_folder, new_file_name)
            os.rename(file_path, new_file_path)

            # Update file count for the class
            if file_class not in file_count_per_class:
                file_count_per_class[file_class] = 0
            file_count_per_class[file_class] += 1

    # Check the number of class folders
    if len(file_count_per_class) != expected_num_classes:
        raise HTTPException(status_code=400, detail=f"Expected {expected_num_classes} class folders, but found {len(file_count_per_class)}.")

    # Check if each class folder contains the correct number of files
    for class_name, file_count in file_count_per_class.items():
        if file_count != expected_num_files_per_class:
            raise HTTPException(status_code=400, detail=f"Class folder '{class_name}' contains {file_count} files, but {expected_num_files_per_class} were expected.")

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

            # Get the name of the zip without the '.zip' extension
            zip_name_without_extension = os.path.splitext(file.filename)[0]

            # Determine the path of the directory matching the ZIP name
            matching_dir_path = os.path.join(temp_dir, zip_name_without_extension)

            # Check if the matching directory (same name as ZIP file) exists and whether it contains any classes (subdirectories)
            if os.path.exists(matching_dir_path) and os.path.isdir(matching_dir_path):
                # List contents of the matching directory
                if any(os.path.isdir(os.path.join(matching_dir_path, item)) for item in os.listdir(matching_dir_path)):
                    # Contains classes (subdirectories), validate as a structured directory with classes
                    await validate_zip_structure(temp_dir, expected_num_classes, expected_num_files_per_class)
                else:
                    # No classes (subdirectories), validate as a flat file structure
                    await validate_flat_zip_structure(matching_dir_path, expected_num_classes, expected_num_files_per_class)
            else:
                # No matching directory or it does not contain directories
                raise HTTPException(status_code=400, detail="Validation failed for the zip contents.")

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