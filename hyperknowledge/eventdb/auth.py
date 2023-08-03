"""FastAPI server authentification"""

from typing import Optional, Annotated
from datetime import datetime, timedelta

import anyio
from datetime import timezone
from sqlalchemy import select
from fastapi import HTTPException, Depends, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from passlib.context import CryptContext
from jose import jwt, JWTError

from .. import config, target_db, Session
from .schemas import (BaseModel, AgentModel, AgentModelWithPw)
from .models import Agent

SECRET_KEY = config.get(target_db, 'auth_secret')
ALGORITHM = "HS256"

class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    id: int

# 2a allows compatibility with pg_crypto if I want to check in DB
pwd_context = CryptContext(schemes=['bcrypt'], deprecated='auto', bcrypt__ident='2a')

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

async def get_agent(session, agent_id: int) -> AgentModel:
      agent = await session.get(Agent, agent_id)
      if agent:
          return AgentModel.model_validate(agent)

async def get_agent_by_username(session, username: str) -> AgentModelWithPw:
      agent = await session.execute(select(Agent).filter_by(username=username))
      if agent := agent.first():
          return AgentModelWithPw.model_validate(agent[0])

async def authenticate_agent(session, username: str, password: str):
    agent = await get_agent_by_username(session, username)
    if not agent:
        return False
    if not verify_password(password, agent.passwd):
        return False
    return agent


def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)
    to_encode["exp"] = expire
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


async def get_current_agent(token: Annotated[str, Depends(oauth2_scheme)]) -> AgentModel:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        if not token:
            return None
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        subtype, subid = payload.get("sub").split(':', 1)
        assert subtype == 'agent'
        user_id: int = int(subid)
        if user_id is None:
            raise credentials_exception
        token_data = TokenData(id=user_id)
    except JWTError as e:
        raise credentials_exception from e
    async with Session() as session:
        agent = await get_agent(session, agent_id=token_data.id)
    if agent is None:
        raise credentials_exception
    return agent


async def get_current_active_agent(
    current_agent: Annotated[AgentModelWithPw, Depends(get_current_agent)]
) -> AgentModel:
    if not current_agent:
        raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
          detail="Please login",
          headers={"WWW-Authenticate": "Bearer"})
    if not current_agent.confirmed:
        raise HTTPException(status_code=400, detail="Not confirmed")
    return current_agent
