from fastapi import APIRouter
from ..controllers import file_controller

router = APIRouter()

router.post("/upload-file/{bucket_name}")(file_controller.upload_file)
router.get("/download-file/{bucket_name}/{file_name}")(file_controller.download_file)
router.delete("/delete-file/{bucket_name}/{file_name}")(file_controller.delete_file)
