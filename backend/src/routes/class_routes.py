from fastapi import APIRouter, Depends
from ..controllers import class_controller
from ..utils.auth import get_current_user  

router = APIRouter()

router.post("/upload-class/{bucket_name}/{database_name}", dependencies=[Depends(get_current_user)])(class_controller.upload_class)
router.get("/get-all-classes/{bucket_name}/{database_name}", dependencies=[Depends(get_current_user)])(class_controller.get_all_classes)
router.get("/download-class/{bucket_name}/{database_name}/{class_name}", dependencies=[Depends(get_current_user)])(class_controller.download_class)
router.delete("/delete-class/{bucket_name}/{database_name}/{class_name}", dependencies=[Depends(get_current_user)])(class_controller.delete_class)
