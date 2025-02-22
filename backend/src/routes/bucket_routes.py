from fastapi import APIRouter, Depends
from ..controllers import bucket_controller
from ..utils.auth import get_current_user  

router = APIRouter()

router.post("/create-bucket/{bucket_name}", dependencies=[Depends(get_current_user)])(bucket_controller.create_bucket)
router.get("/get-all-buckets", dependencies=[Depends(get_current_user)])(bucket_controller.get_all_buckets)
router.delete("/delete-bucket/{bucket_name}", dependencies=[Depends(get_current_user)])(bucket_controller.delete_bucket)