import logging
import os
from fastapi import File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from minio import S3Error
from ..utils.minio_client import get_minio_client
from ..utils.minio_validators import check_bucket_exists, check_class_exists, check_directory_exists, check_sample_exists


client = get_minio_client()
logger = logging.getLogger(__name__)

async def upload_sample(bucket_name: str, directory_name: str, class_name: str, 
                        file: UploadFile = File(...)):
    """
    Uploads a sample file with a .bmp extension to a specified class within a directory in an S3 bucket.

    :param bucket_name: The name of the S3 bucket.
    :param directory_name: The name of the directory containing the class.
    :param class_name: The name of the class (subdirectory) to receive the file.
    :param file: The sample file to upload.
    :return: A message indicating the status of the upload.
    """
    file_extension = os.path.splitext(file.filename)[1]
    if file_extension != '.bmp':
        raise HTTPException(status_code=400, detail="Only .bmp files are accepted.")

    await check_bucket_exists(bucket_name)
    await check_directory_exists(bucket_name, directory_name)
    await check_class_exists(bucket_name, directory_name, class_name)

    file_path = f"{directory_name}/{class_name}/{file.filename}"
    
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
    
async def get_all_samples(bucket_name: str, directory_name: str, class_name: str):
    """
    Retrieves all samples (files) from a specified class within a directory in an S3 bucket.

    :param bucket_name: The name of the S3 bucket.
    :param directory_name: The name of the directory within the bucket.
    :param class_name: The name of the class (subfolder) within the directory.
    :return: A list of file names (objects) within the specified class.
    :raises HTTPException: If there is an error in fetching the samples.
    """
    await check_bucket_exists(bucket_name)
    await check_directory_exists(bucket_name, directory_name)
    await check_class_exists(bucket_name, directory_name, class_name)

    if not directory_name.endswith('/'):
        directory_name += '/'
    if not class_name.endswith('/'):
        class_name += '/'
    full_prefix = f"{directory_name}{class_name}"

    try:
        objects = client.list_objects(bucket_name, prefix=full_prefix, recursive=True)
        # Create a list to store the names of the objects, stripping the prefix from each name
        sample_list = [obj.object_name.replace(full_prefix, "") for obj in objects if obj.object_name != full_prefix and not obj.is_dir]
        return {"samples": sample_list}
    except Exception as e:
        logger.error(f"Failed to retrieve samples from '{class_name}' in '{directory_name}' of bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve samples: {str(e)}")
    
async def download_sample(bucket_name: str, directory_name: str, class_name: str, sample_name: str):
    """
    Downloads a specific sample from a class within a directory in an S3 bucket.

    :param bucket_name: The name of the S3 bucket.
    :param directory_name: The directory within the bucket.
    :param class_name: The class (subdirectory) within the directory.
    :param sample_name: The name of the sample (file) to download.
    :return: The file as a download.
    """

    await check_bucket_exists(bucket_name)
    await check_directory_exists(bucket_name, directory_name)
    await check_class_exists(bucket_name, directory_name, class_name)
    await check_sample_exists(bucket_name, directory_name, class_name, sample_name)

    file_path = f"{directory_name}/{class_name}/{sample_name}"
    temp_file_path = f"/tmp/{sample_name}"

    try:
        client.fget_object(bucket_name, file_path, temp_file_path)
        return FileResponse(path=temp_file_path, filename=sample_name, media_type='application/octet-stream')
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
async def delete_sample(bucket_name: str, directory_name: str, class_name: str, sample_name: str):
    """
    Deletes a specific sample (file) within a class from an S3 bucket.

    :param bucket_name: The name of the S3 bucket.
    :param directory_name: The name of the directory containing the class.
    :param class_name: The name of the class (subdirectory) containing the sample.
    :param sample_name: The name of the sample (file) to delete.
    :return: A message indicating the status of the deletion.
    """
    try:
        await check_bucket_exists(bucket_name)
        await check_directory_exists(bucket_name, directory_name)
        await check_class_exists(bucket_name, directory_name, class_name)
        await check_sample_exists(bucket_name, directory_name, class_name, sample_name)

        file_path = f"{directory_name}/{class_name}/{sample_name}"
        client.remove_object(bucket_name, file_path)

        logger.info(f"Sample '{sample_name}' deleted successfully from class '{class_name}' in bucket '{bucket_name}'.")
        return {"message": f"Sample '{sample_name}' deleted successfully from class '{class_name}' in directory '{directory_name}' in bucket '{bucket_name}'."}

    except S3Error as e:
        logger.error(f"Failed to delete sample '{sample_name}' from class '{class_name}' in directory '{directory_name}' in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete sample: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error occurred while deleting sample '{sample_name}' from class '{class_name}' in directory '{directory_name}' in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")