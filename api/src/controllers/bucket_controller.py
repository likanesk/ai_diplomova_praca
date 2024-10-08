from fastapi import HTTPException
from minio.error import S3Error
from ..utils.minio_validators import check_bucket_exists
from ..utils.minio_client import get_minio_client

client = get_minio_client()

async def create_bucket(bucket_name: str):
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            return {"message": f"Bucket '{bucket_name}' created successfully."}
        else:
            return {"message": f"Bucket '{bucket_name}' already exists."}
    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"Failed to create bucket: {str(e)}")

async def get_all_buckets():
    try:
        buckets = client.list_buckets()
        return {
            "buckets": [
                {"name": bucket.name, "creation_date": bucket.creation_date}
                for bucket in buckets
            ]
        }
    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"Failed to list buckets: {str(e)}")

async def delete_bucket(bucket_name: str):
    try:
        try:
            await check_bucket_exists(bucket_name)
            return {"message": f"Bucket '{bucket_name}' already exists."}
        except HTTPException as e:
            if e.status_code == 404:
                client.make_bucket(bucket_name)
                return {"message": f"Bucket '{bucket_name}' created successfully."}
            else:
                raise e
    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"Failed to create bucket: {str(e)}")

async def delete_bucket(bucket_name: str):
    try:
        await check_bucket_exists(bucket_name)

        objects = client.list_objects(bucket_name)
        for obj in objects:
            client.remove_object(bucket_name, obj.object_name)

        client.remove_bucket(bucket_name)
        return {"message": f"Bucket '{bucket_name}' deleted successfully."}
    
    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete bucket: {str(e)}")