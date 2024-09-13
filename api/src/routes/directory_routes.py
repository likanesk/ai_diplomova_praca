from fastapi import APIRouter
from ..controllers import directory_controller

router = APIRouter()

router.post("/upload-zip/{bucket_name}")(directory_controller.upload_zip)
