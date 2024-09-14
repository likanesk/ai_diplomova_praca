from fastapi import FastAPI
from src.routes.bucket_routes import router as bucket_router
from src.routes.file_routes import router as file_router
from src.routes.directory_routes import router as directory_router
from src.routes.class_routes import router as class_router
from src.routes.sample_routes import router as sample_router

app = FastAPI()

app.include_router(bucket_router, tags=["Buckets"], prefix="/buckets")
app.include_router(file_router, tags=["Files"], prefix="/files")
app.include_router(directory_router, tags=["Directories"], prefix="/directories")
app.include_router(class_router, tags=["Classes (Subdirectories)"], prefix="/classes")
app.include_router(sample_router, tags=["Samples"], prefix="/samples")