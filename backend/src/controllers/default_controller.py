import base64
import json
import logging
import os
from fastapi import HTTPException
from minio import Minio
from minio.error import S3Error

from ..utils.minio_validators import check_bucket_exists, check_class_exists, check_database_exists

logger = logging.getLogger(__name__)

def connect_minio(endpoint: str, access_key: str, secret_key: str, secure: bool = True):
    """
    Initializes a connection to MinIO.

    :param endpoint: MinIO server URL (e.g., "play.min.io:9000").
    :param access_key: Access key.
    :param secret_key: Secret key.
    :param secure: Use HTTPS (True) or HTTP (False).
    :return: MinIO client.
    """
    try:
        client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )
        return client
    except S3Error as e:
        print(f"Error connecting to MinIO: {e}")
        raise

async def _process_sample_object(client: Minio, bucket_name: str, object_path: str, class_name: str = None):
    """
    Helper function to process a sample object and return its data.
    Automatically looks for matching JSON data and adds ROI/regression values if found.
    Class is included only if provided in class_name or found in path.
    Returns error if multiple JSON files are found in the directory.
    
    :param client: MinIO client
    :param bucket_name: Bucket name
    :param object_path: Full path to the object in MinIO
    :param class_name: Optional class name (if known)
    :return: Tuple of (processed sample data, error message) 
    """
    response = None
    try:
        response = client.get_object(bucket_name, object_path)
        data = response.read()
        
        # Determine class from path if not provided
        actual_class = class_name if class_name else None
        if not actual_class:
            path_parts = object_path.split('/')
            if len(path_parts) > 2:
                actual_class = path_parts[1]

        result = {
            "path": object_path,
            "file": os.path.basename(object_path),
            "data": base64.b64encode(data).decode('utf-8'),
            "metadata": {},
            "size_bytes": len(data)
        }

        if actual_class:
            result["class"] = actual_class

        # Process metadata
        for k, v in response.headers.items():
            try:
                result["metadata"][k] = v.decode('utf-8') if isinstance(v, bytes) else str(v)
            except Exception as e:
                logger.error(f"Failed to serialize metadata key '{k}' for object '{object_path}': {str(e)}")
                result["metadata"][k] = "[non-serializable value]"

        # Look for matching JSON file
        dir_path = os.path.dirname(object_path)
        if not dir_path.endswith('/'):
            dir_path += '/'
        
        try:
            objects = client.list_objects(bucket_name, prefix=dir_path, recursive=False)
            json_files = [
                obj.object_name for obj in objects
                if not obj.object_name.endswith('/') 
                and obj.object_name.lower().endswith('.json')
                and os.path.dirname(obj.object_name) == dir_path.rstrip('/')
            ]

            print(json_files)
            
            if len(json_files) > 1:
                error_msg = f"Directory contains multiple JSON files for object '{object_path}'"
                logger.error(error_msg)
                raise HTTPException(status_code=500, detail=error_msg)
            
            if json_files:
                json_path = json_files[0]
                try:
                    json_response = client.get_object(bucket_name, json_path)
                    json_data = json.loads(json_response.read().decode('utf-8'))
                    
                    if isinstance(json_data, list):
                        for entry in json_data:
                            if entry.get("file") == result["file"]:
                                if "ROI" in entry:
                                    result["ROI"] = entry["ROI"]
                                if "regression" in entry:
                                    result["regression"] = entry["regression"]
                                break
                    
                    json_response.close()
                except Exception as e:
                    logger.error(f"Failed to process JSON file '{json_path}' for object '{object_path}': {str(e)}")
                    raise HTTPException(status_code=500, detail=f"Failed to process JSON file: {str(e)}")

        except Exception as e:
            logger.error(f"Failed to list objects in directory '{dir_path}' for object '{object_path}': {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to access object metadata: {str(e)}")

        return result

    except Exception as e:
        logger.error(f"Failed to process object '{object_path}' in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to process object: {str(e)}")
    finally:
        if response:
            response.close()
            response.release_conn()

async def get_sample_by_name(
    client: Minio,
    bucket_name: str,
    database_name: str,
    sample_name: str,
    class_name: str = None,
    exact_match: bool = True
):
    """
    Retrieves a single sample by its name in the dataset/class.
    
    :param client: MinIO client
    :param bucket_name: Bucket name
    :param database_name: Dataset name (folder in bucket)
    :param sample_name: Name of the sample to retrieve
    :param class_name: Optional class name (subfolder)
    :param exact_match: Whether to match the exact filename (True) or allow partial matches (False)
    :return: Sample data with path, data, metadata, class, and size
    """
    try:
        await check_bucket_exists(client, bucket_name)
        await check_database_exists(client, bucket_name, database_name)
        
        if class_name:
            await check_class_exists(client, bucket_name, database_name, class_name)

        prefix = f"{database_name}/{class_name}/" if class_name else f"{database_name}/"
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        
        # Find matching samples
        matching_samples = []
        for obj in objects:
            if not obj.is_dir:
                obj_name = obj.object_name.split('/')[-1]
                if (exact_match and obj_name == sample_name) or (not exact_match and sample_name in obj_name):
                    matching_samples.append(obj.object_name)
        
        if not matching_samples:
            logger.error(f"Sample '{sample_name}' not found in database '{database_name}' of bucket '{bucket_name}'")
            raise HTTPException(status_code=404, detail=f"Sample '{sample_name}' not found")
        
        if len(matching_samples) > 1:
            logger.error(f"Multiple samples match '{sample_name}' in database '{database_name}' of bucket '{bucket_name}': {matching_samples}")
            raise HTTPException(status_code=400, detail=f"Multiple samples match '{sample_name}'")
            
        # Process the matching sample
        return await _process_sample_object(client, bucket_name, matching_samples[0], class_name)
        
    except S3Error as e:
        logger.error(f"S3 error while retrieving sample '{sample_name}' from database '{database_name}' in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Storage error: {str(e)}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error while retrieving sample '{sample_name}' from database '{database_name}' in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    
async def get_sample_by_index(
    client: Minio,
    bucket_name: str,
    database_name: str,
    index: int,
    class_name: str = None
):
    """
    Retrieves a single sample by its index position in the dataset/class.
    
    :param client: MinIO client
    :param bucket_name: Bucket name
    :param database_name: Dataset name (folder in bucket)
    :param index: Zero-based index of the sample to retrieve
    :param class_name: Optional class name (subfolder)
    :return: Sample data with path, data, metadata, class, and size
    """
    try:
        logger.info(f"Retrieving sample by index {index} from database '{database_name}' in bucket '{bucket_name}'"
                   f"{f', class {class_name}' if class_name else ''}")

        await check_bucket_exists(client, bucket_name)
        await check_database_exists(client, bucket_name, database_name)
        
        if class_name:
            await check_class_exists(client, bucket_name, database_name, class_name)

        prefix = f"{database_name}/{class_name}/" if class_name else f"{database_name}/"
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        
        # Filter out directories and get object names
        samples = [obj.object_name for obj in objects if not obj.is_dir]
        
        if index < 0 or index >= len(samples):
            error_msg = f"Index {index} out of range (0-{len(samples)-1}) in database '{database_name}'"
            logger.error(f"{error_msg} in bucket '{bucket_name}'")
            raise HTTPException(
                status_code=400,
                detail=error_msg
            )
            
        logger.debug(f"Found sample at index {index}: {samples[index]}")
        return await _process_sample_object(client, bucket_name, samples[index], class_name)
        
    except S3Error as e:
        logger.error(f"S3 error retrieving sample by index from database '{database_name}' in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Storage error: {str(e)}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error retrieving sample by index from database '{database_name}' in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

async def get_samples_by_count(
    client: Minio,
    bucket_name: str,
    database_name: str,
    index: int,
    count: int,
    class_name: str = None
):
    """
    Retrieves multiple samples from MinIO based on the provided index and count.
    Searches in the dataset and its subfolders (e.g., class folders).

    :param client: MinIO client.
    :param bucket_name: Name of the bucket.
    :param database_name: Name of the dataset (folder in the bucket).
    :param index: Starting index of the samples.
    :param count: Number of samples to retrieve.
    :param class_name: Optional subfolder within the dataset (e.g., class name).
    :return: A list of dictionaries, each containing sample data and metadata.
    :raises HTTPException: If the requested count exceeds the available samples or if parameters are invalid.
    """
    try:
        logger.info(f"Retrieving {count} samples starting from index {index} from database '{database_name}'"
                   f" in bucket '{bucket_name}'{f', class {class_name}' if class_name else ''}")

        await check_bucket_exists(client, bucket_name)
        await check_database_exists(client, bucket_name, database_name)
        
        if class_name:
            await check_class_exists(client, bucket_name, database_name, class_name)

        prefix = f"{database_name}/{class_name}/" if class_name else f"{database_name}/"
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        
        samples = [obj.object_name for obj in objects if not obj.is_dir]

        # Validate parameters
        if index < 0:
            error_msg = f"Invalid index {index} (must be >= 0)"
            logger.error(f"{error_msg} in database '{database_name}' of bucket '{bucket_name}'")
            raise HTTPException(
                status_code=400,
                detail=error_msg
            )
            
        if index >= len(samples):
            error_msg = f"Index {index} exceeds available samples ({len(samples)})"
            logger.error(f"{error_msg} in database '{database_name}' of bucket '{bucket_name}'")
            raise HTTPException(
                status_code=400,
                detail=error_msg
            )
        
        # Check if count is valid
        if count <= 0:
            error_msg = f"Invalid count {count} (must be > 0)"
            logger.error(f"{error_msg} in database '{database_name}' of bucket '{bucket_name}'")
            raise HTTPException(
                status_code=400,
                detail=error_msg
            )

        # Check if index + count exceeds the number of available samples
        if index + count > len(samples):
            error_msg = f"Requested {count} samples from index {index} but only {len(samples)-index} available"
            logger.error(f"{error_msg} in database '{database_name}' of bucket '{bucket_name}'")
            raise HTTPException(
                status_code=400,
                detail=error_msg
            )

        # Process the requested samples
        result = []
        for i, sample_path in enumerate(samples[index:index + count]):
            try:
                sample_data = await _process_sample_object(client, bucket_name, sample_path, class_name)
                result.append(sample_data)
                logger.debug(f"Processed sample {i+1}/{count}: {sample_path}")
            except Exception as e:
                logger.error(f"Failed to process sample {sample_path}: {str(e)}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to process sample at index {index + i}: {str(e)}"
                )
        
        logger.info(f"Successfully retrieved {len(result)} samples")
        return result
        
    except S3Error as e:
        logger.error(f"S3 error retrieving samples from database '{database_name}' in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Storage error: {str(e)}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error retrieving samples from database '{database_name}' in bucket '{bucket_name}': {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )