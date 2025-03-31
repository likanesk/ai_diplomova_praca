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

async def get_sample(client, bucket_name: str, database_name: str, sample_name: str, class_name: str = None):
    """
    Retrieves a specific sample by object path created by database_name/sample_name or database_name/class_name/sample_name from MinIO.
    Searches in the dataset and its subfolders (e.g., class folders).

    :param client: MinIO client.
    :param bucket_name: Name of the bucket.
    :param database_name: Name of the dataset (folder in the bucket).
    :param sample_name: Name of the sample (file).
    :param class_name: Optional subfolder within the dataset (e.g., class name).
    :return: A dictionary containing the sample data and metadata.
    :raises HTTPException: If the sample is not found or if an error occurs.
    """
    try:
        await check_bucket_exists(client, bucket_name)
        await check_database_exists(client, bucket_name, database_name)       

        if class_name:
            await check_class_exists(client, bucket_name, database_name, class_name)

        if class_name:
            object_path = f"{database_name}/{class_name}/{sample_name}"
        else:
            object_path = f"{database_name}/{sample_name}"

        response = client.get_object(bucket_name, object_path)
        
        return {
            "path": object_path,
            "data": response.data,
            "metadata": response.headers,
        }
    except S3Error as e:
        logger.error(f"Error retrieving sample: {e}")
        raise HTTPException(status_code=404, detail=f"Failed to retrieve sample: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")
    
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

    Behavior:
    - If `class_name` is provided:
        - The function retrieves samples only from the specified class.
        - If `index + count` exceeds the number of samples in the class, an HTTPException is raised.
    - If `class_name` is not provided:
        - If the dataset contains classes:
            - The function retrieves samples from all classes, starting from the first class.
            - If `index + count` exceeds the number of samples in the current class, the function continues to the next class
              until the requested number of samples is retrieved or no more samples are available.
        - If the dataset does not contain classes:
            - The function retrieves samples directly from the dataset.
            - If `index + count` exceeds the total number of samples in the dataset, an HTTPException is raised.
    """
    try:
        await check_bucket_exists(client, bucket_name)
        await check_database_exists(client, bucket_name, database_name)
        
        if class_name:
            await check_class_exists(client, bucket_name, database_name, class_name)

        if class_name:
            prefix = f"{database_name}/{class_name}/"
        else:
            prefix = f"{database_name}/"

        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        
        samples = [obj.object_name for obj in objects if not obj.is_dir]

        # Check if index is valid
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

        start_index = index
        end_index = index + count
        paginated_samples = samples[start_index:end_index]

        result = []
        for sample_path in paginated_samples:
            response = client.get_object(bucket_name, sample_path)
            
            # Create base result dictionary
            sample_result = {
                "path": sample_path,
                "data": response.data,
                "metadata": response.headers
            }
            
            # Add class only if it exists in the path or was explicitly requested
            if class_name:
                sample_result["class"] = class_name
            else:
                path_parts = sample_path.split('/')
                if len(path_parts) > 2:  # If there's a class in the path
                    sample_result["class"] = path_parts[1]
            
            result.append(sample_result)
        
        return result
    except S3Error as e:
        logger.error(f"Error retrieving samples: {e}")
        raise HTTPException(status_code=404, detail=f"Failed to retrieve samples: {e}")
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")