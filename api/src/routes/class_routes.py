from fastapi import APIRouter
from ..controllers import class_controller

router = APIRouter()

router.post("/upload-class/{bucket_name}/{directory_name}")(class_controller.upload_class)
router.get("/get-all-classes/{bucket_name}/{directory_name}")(class_controller.get_all_classes)
router.get("/download-class/{bucket_name}/{directory_name}/{class_name}")(class_controller.download_class)
router.delete("/delete-class/{bucket_name}/{file_name}/{class_name}")(class_controller.delete_class)
