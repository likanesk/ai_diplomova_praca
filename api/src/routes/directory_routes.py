from fastapi import APIRouter
from ..controllers import directory_controller

router = APIRouter()

router.post("/upload-zip/{bucket_name}")(directory_controller.upload_zip)
router.get("/download-directory/{bucket_name}/{directory_name}")(directory_controller.download_directory)
router.get("/download-class/{bucket_name}/{directory_name}/{class_name}")(directory_controller.download_class)
