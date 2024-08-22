from minio import Minio
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

minio_endpoint = os.getenv("MINIO_ENDPOINT")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
minio_secure = os.getenv("MINIO_SECURE", "false").lower() == 'true'

if not minio_endpoint or not minio_access_key or not minio_secret_key:
    raise ValueError("Minio configuration is missing in the environment variables.")

client = Minio(
    minio_endpoint,
    access_key=minio_access_key,
    secret_key=minio_secret_key,
    secure=minio_secure
)

def get_minio_client():
    return client
