import logging
from fastapi import UploadFile, File, Form, HTTPException
from minio.error import S3Error
from ..utils.minio_validators import check_bucket_exists, check_file_exists
from ..utils.minio_client import get_minio_client

client = get_minio_client() 

logger = logging.getLogger(__name__)

async def upload_file(
    bucket_name: str,
    file: UploadFile = File(...),
    metadata: str = Form(...)
):
    try:
        check_bucket_exists(bucket_name)
        
        try:
            client.stat_object(bucket_name, file.filename)
            file_exists = True
        except S3Error as e:
            if "NoSuchKey" in str(e):
                file_exists = False
            else:
                logger.error(f"Error while checking if file '{file.filename}' exists in bucket '{bucket_name}': {str(e)}")
                raise e
        
        file.file.seek(0)
        file_size = file.file.seek(0, 2)
        file.file.seek(0)
        
        client.put_object(
            bucket_name,
            file.filename,
            data=file.file,
            length=file_size,
            metadata={"x-amz-meta-metadata": metadata}
        )
        
        if file_exists:
            logger.info(f"File '{file.filename}' was overwritten in bucket '{bucket_name}'.")
            return {"message": f"File '{file.filename}' already existed and was overwritten."}
        else:
            logger.info(f"File '{file.filename}' uploaded successfully to bucket '{bucket_name}'.")
            return {"message": "File uploaded successfully with metadata."}
    
    except S3Error as e:
        logger.error(f"Failed to upload file '{file.filename}' to bucket '{bucket_name}': {str(e)}")
        return {"error": f"Failed to upload file: {str(e)}"}
    except Exception as e:
        logger.error(f"Unexpected error occurred while uploading file '{file.filename}' to bucket '{bucket_name}': {str(e)}")
        return {"error": str(e)}

async def download_file(bucket_name: str, file_name: str):
    try:
        await check_bucket_exists(bucket_name)
        
        file_location = f"/tmp/{file_name}"
        client.fget_object(bucket_name, file_name, file_location)
        
        logger.info(f"File '{file_name}' downloaded successfully from bucket '{bucket_name}'.")
        return {"message": "File downloaded successfully.", "file_location": file_location}
    except S3Error as e:
        logger.error(f"Failed to download file '{file_name}' from bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to download file: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error occurred while downloading file '{file_name}' from bucket '{bucket_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

async def delete_file(bucket_name: str, file_name: str):
    try:
        await check_bucket_exists(bucket_name)
        await check_file_exists(bucket_name, file_name)
        
        client.remove_object(bucket_name, file_name)
        logger.info(f"File '{file_name}' deleted successfully from bucket '{bucket_name}'.")
        return {"message": f"File '{file_name}' deleted successfully."}
    
    except S3Error as e:
        logger.error(f"S3Error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete file: {str(e)}")
    
    except HTTPException as e:
        logger.error(f"HTTP error: {str(e.detail)}")
        raise e
    
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")