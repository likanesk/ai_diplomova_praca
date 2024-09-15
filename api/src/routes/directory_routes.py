from fastapi import APIRouter
from ..controllers import directory_controller

router = APIRouter()

router.post("/upload-zip/{bucket_name}")(directory_controller.upload_zip)
router.get("/get-all-directories/{bucket_name}")(directory_controller.get_all_directories)
router.get("/download-directory/{bucket_name}/{directory_name}")(directory_controller.download_directory)
router.delete("/delete-directory/{bucket_name}/{file_name}")(directory_controller.delete_directory)
