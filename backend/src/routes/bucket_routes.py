from fastapi import APIRouter
from ..controllers import bucket_controller

router = APIRouter()

router.post("/create-bucket/{bucket_name}")(bucket_controller.create_bucket)
router.get("/get-all-buckets")(bucket_controller.get_all_buckets)
router.delete("/delete-bucket/{bucket_name}")(bucket_controller.delete_bucket)
