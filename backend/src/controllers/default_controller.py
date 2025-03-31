import base64
import logging
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
    
    :param client: MinIO client
    :param bucket_name: Bucket name
    :param object_path: Full path to the object in MinIO
    :param class_name: Optional class name (if known)
    :return: Processed sample data
    """
    response = None
    try:
        response = client.get_object(bucket_name, object_path)
        data = response.read()
        
        # Determine class from path if not provided
        actual_class = class_name
        if not class_name:
            path_parts = object_path.split('/')
            if len(path_parts) > 2:  # If there's a class in the path
                actual_class = path_parts[1]

        # Convert metadata to serializable format
        metadata = {}
        for k, v in response.headers.items():
            try:
                # Try to decode if it's bytes
                if isinstance(v, bytes):
                    metadata[k] = v.decode('utf-8')
                else:
                    metadata[k] = str(v)
            except:
                metadata[k] = "[non-serializable value]"

        return {
            "path": object_path,
            "data": base64.b64encode(data).decode('utf-8'),  # Base64 encoded string
            "metadata": metadata,
            "class": actual_class,
            "size_bytes": len(data)
        }
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
                obj_name = obj.object_name.split('/')[-1]  # Get just the filename
                if exact_match:
                    if obj_name == sample_name:
                        matching_samples.append(obj.object_name)
                else:
                    if sample_name in obj_name:
                        matching_samples.append(obj.object_name)
        
        if not matching_samples:
            raise HTTPException(
                status_code=404,
                detail=f"Sample '{sample_name}' not found in {prefix}"
            )
        
        if len(matching_samples) > 1:
            raise HTTPException(
                status_code=400,
                detail=f"Multiple samples match '{sample_name}': {matching_samples}"
            )
            
        # Process the matching sample
        return await _process_sample_object(client, bucket_name, matching_samples[0], class_name)
        
    except S3Error as e:
        logger.error(f"Error listing samples: {e}")
        raise HTTPException(status_code=404, detail=f"Failed to list samples: {e}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")

async def get_sample_by_index(client, bucket_name: str, database_name: str, index: int, class_name: str = None):
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
        await check_bucket_exists(client, bucket_name)
        await check_database_exists(client, bucket_name, database_name)
        
        if class_name:
            await check_class_exists(client, bucket_name, database_name, class_name)

        prefix = f"{database_name}/{class_name}/" if class_name else f"{database_name}/"
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        
        # Filter out directories and get object names
        samples = [obj.object_name for obj in objects if not obj.is_dir]
        
        if index < 0 or index >= len(samples):
            raise HTTPException(
                status_code=400,
                detail=f"Index {index} is out of range. Available samples: {len(samples)}"
            )
            
        # Process the sample at the specified index
        return await _process_sample_object(client, bucket_name, samples[index], class_name)
        
    except S3Error as e:
        logger.error(f"Error listing samples: {e}")
        raise HTTPException(status_code=404, detail=f"Failed to list samples: {e}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")
    
async def get_samples_by_count(client, bucket_name: str, database_name: str, index: int, count: int, class_name: str = None):
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
        await check_bucket_exists(client, bucket_name)
        await check_database_exists(client, bucket_name, database_name)
        
        if class_name:
            await check_class_exists(client, bucket_name, database_name, class_name)

        prefix = f"{database_name}/{class_name}/" if class_name else f"{database_name}/"
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        
        samples = [obj.object_name for obj in objects if not obj.is_dir]

        # Validate parameters
        if index < 0:
            raise HTTPException(
                status_code=400,
                detail=f"Index cannot be negative. Provided index: {index}."
            )
        if index >= len(samples):
            raise HTTPException(
                status_code=400,
                detail=f"Index {index} is out of range. There are only {len(samples)} samples available."
            )

        # Check if count is valid
        if count <= 0:
            raise HTTPException(
                status_code=400,
                detail=f"Count must be greater than 0. Provided count: {count}."
            )
        if count > len(samples):
            raise HTTPException(
                status_code=400,
                detail=f"Requested {count} samples, but only {len(samples)} samples are available."
            )

        # Check if index + count exceeds the number of available samples
        if index + count > len(samples):
            raise HTTPException(
                status_code=400,
                detail=f"Requested {count} samples starting from index {index}, but only {len(samples)} samples are available."
            )

        # Process the requested samples
        result = []
        for sample_path in samples[index:index + count]:
            sample_data = await _process_sample_object(client, bucket_name, sample_path, class_name)
            result.append(sample_data)
        
        return result
        
    except S3Error as e:
        logger.error(f"Error retrieving samples: {e}")
        raise HTTPException(status_code=404, detail=f"Failed to retrieve samples: {e}")
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")