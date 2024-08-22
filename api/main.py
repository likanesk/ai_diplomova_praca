from fastapi import FastAPI
from src.routes.bucket_routes import router as bucket_router

app = FastAPI()

app.include_router(bucket_router)