from fastapi import FastAPI
from src.routes.bucket_routes import router as bucket_router
from src.routes.file_routes import router as file_router
from src.routes.directory_routes import router as directory_router

app = FastAPI()

app.include_router(bucket_router)
app.include_router(file_router)
app.include_router(directory_router)
