import os
import tempfile
import logging
import zipfile
from fastapi import HTTPException, File, UploadFile
from ..utils.minio_client import get_minio_client
from ..utils.minio_validators import check_bucket_exists
from minio.error import S3Error

client = get_minio_client()
logger = logging.getLogger(__name__)

async def upload_zip(bucket_name: str, file: UploadFile = File(...)):
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

    return {"message": "Directory uploaded successfully"}