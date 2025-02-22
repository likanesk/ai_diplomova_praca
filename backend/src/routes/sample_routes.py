from fastapi import APIRouter, Depends
from ..controllers import sample_controller
from ..utils.auth import get_current_user  

router = APIRouter()

router.post("/upload-sample/{bucket_name}/{database_name}/{class_name}", dependencies=[Depends(get_current_user)])(sample_controller.upload_sample)
router.get("/get-all-samples-in-class/{bucket_name}/{database_name}/{class_name}", dependencies=[Depends(get_current_user)])(sample_controller.get_all_samples)
router.get("/download-sample/{bucket_name}/{database_name}/{class_name}/{sample_name}", dependencies=[Depends(get_current_user)])(sample_controller.download_sample)
router.delete("/delete-sample/{bucket_name}/{database_name}/{class_name}/{sample_name}", dependencies=[Depends(get_current_user)])(sample_controller.delete_sample)
