from fastapi import APIRouter
from ..controllers import bucket_controller

router = APIRouter()

router.post("/create-bucket/{bucket_name}")(bucket_controller.create_bucket)
router.delete("/delete-bucket/{bucket_name}")(bucket_controller.delete_bucket)
