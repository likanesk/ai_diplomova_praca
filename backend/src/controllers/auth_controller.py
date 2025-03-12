import subprocess
from fastapi import APIRouter, Depends, HTTPException
from datetime import timedelta
from fastapi import status
from pydantic import BaseModel
from ..utils.config import MINIO_ALIAS, MINIO_SERVER
from ..utils.auth import create_access_token, ACCESS_TOKEN_EXPIRE_MINUTES, get_current_user

router = APIRouter()

class User(BaseModel):
    username: str
    password: str

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

async def verify_token(current_user: str = Depends(get_current_user)):
    return {"message": "Token is valid", "user": current_user}

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