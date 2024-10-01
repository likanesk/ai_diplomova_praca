from fastapi import APIRouter
from ..controllers import database_controller

router = APIRouter()

router.post("/upload-zip/{bucket_name}")(database_controller.upload_zip)
router.get("/get-all-databases/{bucket_name}")(database_controller.get_all_databases)
router.get("/download-database/{bucket_name}/{database_name}")(database_controller.download_database)
router.delete("/delete-database/{bucket_name}/{database_name}")(database_controller.delete_database)
