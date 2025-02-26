from fastapi import APIRouter
from ..controllers import auth_controller

router = APIRouter()

router.post("/register")(auth_controller.register)
router.post("/login")(auth_controller.login)
router.get("/verify")(auth_controller.verify_token)