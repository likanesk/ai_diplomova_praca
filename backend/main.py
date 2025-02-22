from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.routes.bucket_routes import router as bucket_router
from src.routes.database_routes import router as database_router
from src.routes.class_routes import router as class_router
from src.routes.sample_routes import router as sample_router
from src.routes.auth_routes import router as auth_router

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(bucket_router, tags=["Buckets"], prefix="/buckets")
app.include_router(database_router, tags=["Databases"], prefix="/databases")
app.include_router(class_router, tags=["Classes"], prefix="/classes")
app.include_router(sample_router, tags=["Samples"], prefix="/samples")
app.include_router(auth_router, tags=["Authentication"], prefix="/auth")