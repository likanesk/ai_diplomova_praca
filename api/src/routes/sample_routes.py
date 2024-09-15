from fastapi import APIRouter
from ..controllers import sample_controller

router = APIRouter()

router.get("/get-all-samples-in-class/{bucket_name}/{directory_name}/{class_name}")(sample_controller.get_all_samples)
router.get("/download-sample/{bucket_name}/{directory_name}/{class_name}/{sample_name}")(sample_controller.download_sample)
router.delete("/delete-sample/{bucket_name}/{file_name}/{class_name}/{sample_name}")(sample_controller.delete_sample)
