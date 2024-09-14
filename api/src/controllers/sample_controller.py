import logging
from fastapi import HTTPException
from fastapi.responses import FileResponse
from ..utils.minio_client import get_minio_client
from ..utils.minio_validators import check_bucket_exists, check_class_exists, check_directory_exists, check_sample_exists


client = get_minio_client()
logger = logging.getLogger(__name__)

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