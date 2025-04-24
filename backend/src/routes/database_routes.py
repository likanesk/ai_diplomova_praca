from fastapi import APIRouter, Depends
from ..controllers import database_controller
from ..utils.auth import get_current_user  

router = APIRouter()

router.post("/upload-classification/{bucket_name}", dependencies=[Depends(get_current_user)])(database_controller.upload_classification)
router.post("/upload-regression/{bucket_name}", dependencies=[Depends(get_current_user)])(database_controller.upload_regression)
router.post("/upload-detection/{bucket_name}", dependencies=[Depends(get_current_user)])(database_controller.upload_detection)
router.get("/get-all-databases/{bucket_name}", dependencies=[Depends(get_current_user)])(database_controller.get_all_databases)
router.get("/download-database/{bucket_name}/{database_name}", dependencies=[Depends(get_current_user)])(database_controller.download_database)
router.delete("/delete-database/{bucket_name}/{database_name}", dependencies=[Depends(get_current_user)])(database_controller.delete_database)
