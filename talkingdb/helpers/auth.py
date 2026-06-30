from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from talkingdb.clients.sqlite import sqlite_conn
from talkingdb.models.auth.api_key import APIKeyModel

from talkingdb.helpers.client import config
import bcrypt

security = HTTPBearer()


def verify_api_key(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> str:
    api_key = credentials.credentials

    with sqlite_conn() as conn:
        user_email = APIKeyModel.verify(
            conn=conn,
            api_key=api_key,
        )

    if user_email is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "error_code": "UNAUTHORIZED",
                "message": "Invalid API key",
            },
        )
    return user_email


def hash_password(password: str) -> str:
    return bcrypt.hashpw(
        password.encode("utf-8"),
        bcrypt.gensalt()
    ).decode("utf-8")


def verify_password(
    plain_password: str,
    hashed_password: str,
) -> bool:
    return bcrypt.checkpw(
        plain_password.encode("utf-8"),
        hashed_password.encode("utf-8")
    )