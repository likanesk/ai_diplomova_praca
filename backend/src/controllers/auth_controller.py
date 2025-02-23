import os
import subprocess
from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import OAuth2PasswordRequestForm
from datetime import timedelta
from fastapi import status
from pydantic import BaseModel
from ..utils.config import MINIO_ACCESS_KEY, MINIO_ALIAS, MINIO_SECRET_KEY, MINIO_SERVER
from ..utils.auth import create_access_token, ACCESS_TOKEN_EXPIRE_MINUTES

router = APIRouter()

class User(BaseModel):
    username: str
    password: str

async def register(user: User):
    try:
        setup_minio_alias()

        if user_exists(user.username):
            raise HTTPException(status_code=400, detail=f"User {user.username} already exists.")

        subprocess.run(
            ["mc", "admin", "user", "add", MINIO_ALIAS, user.username, user.password],
            check=True
        )

        return {"message": f"User {user.username} was successfully registered."}

    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail=f"Error registering user: {e}")

async def login(user: User):
    if not authenticate_user(user.username, user.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Generate JWT token
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

def user_exists(username: str) -> bool:
    try:
        result = subprocess.run(
            ["mc", "admin", "user", "list", MINIO_ALIAS],
            check=True,
            capture_output=True,
            text=True
        )
        return username in result.stdout
    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail=f"Error checking user existence: {e}")

def setup_minio_alias():
    try:
        subprocess.run(
            ["mc", "alias", "set", MINIO_ALIAS, MINIO_SERVER, MINIO_ACCESS_KEY, MINIO_SECRET_KEY],
            check=True,
            capture_output=True,
            text=True
        )
        print(f"Alias {MINIO_ALIAS} was successfully set.")
    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail=f"Error setting alias: {e}")

def authenticate_user(username: str, password: str) -> bool:
    try:
        subprocess.run(
            ["mc", "alias", "set", MINIO_ALIAS, MINIO_SERVER, username, password],
            check=True,
            capture_output=True,
            text=True
        )
        return True
    except subprocess.CalledProcessError:
        return False