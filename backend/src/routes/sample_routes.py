from fastapi import APIRouter, Depends
from ..controllers import sample_controller
from ..utils.auth import get_current_user  

router = APIRouter()

router.post("/upload-sample/{bucket_name}/{database_name}/{class_name}", dependencies=[Depends(get_current_user)])(sample_controller.upload_sample)
router.get("/get-all-samples-in-class/{bucket_name}/{database_name}/{class_name}", dependencies=[Depends(get_current_user)])(sample_controller.get_all_samples_for_classification)
router.get("/get-all-samples-in-database/{bucket_name}/{database_name}", dependencies=[Depends(get_current_user)])(sample_controller.get_all_samples_for_regression_detection)
router.get("/download-sample-classification/{bucket_name}/{database_name}/{class_name}/{sample_name}", dependencies=[Depends(get_current_user)])(sample_controller.download_sample)
router.get("/download-sample-detection-regression/{bucket_name}/{database_name}/{sample_name}", dependencies=[Depends(get_current_user)])(sample_controller.download_sample)
router.delete("/delete-sample-classification/{bucket_name}/{database_name}/{class_name}/{sample_name}", dependencies=[Depends(get_current_user)])(sample_controller.delete_sample)
router.delete("/delete-sample-detection-regression/{bucket_name}/{database_name}/{sample_name}", dependencies=[Depends(get_current_user)])(sample_controller.delete_sample)
