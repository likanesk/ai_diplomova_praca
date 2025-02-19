from fastapi import APIRouter
from ..controllers import sample_controller

router = APIRouter()

router.post("/upload-sample/{bucket_name}/{database_name}/{class_name}")(sample_controller.upload_sample)
router.get("/get-all-samples-in-class/{bucket_name}/{database_name}/{class_name}")(sample_controller.get_all_samples)
router.get("/download-sample/{bucket_name}/{database_name}/{class_name}/{sample_name}")(sample_controller.download_sample)
router.delete("/delete-sample/{bucket_name}/{database_name}/{class_name}/{sample_name}")(sample_controller.delete_sample)
