"""FastAPI server authentification"""

from typing import Optional, Annotated
from datetime import datetime, timedelta
from contextlib import asynccontextmanager, suppress

import anyio
from datetime import timezone
from sqlalchemy import select, text
from sqlalchemy.sql.functions import func
from fastapi import HTTPException, Depends, status
from fastapi.security import OAuth2PasswordBearer
from passlib.context import CryptContext
from jose import jwt, JWTError

from .. import db_config_get, client_scoped_session
from .schemas import (BaseModel, AgentModel, AgentModelWithPw, AgentModelOptional)
from .models import Agent

SECRET_KEY = db_config_get('auth_secret')
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


async def get_current_agent(token: Annotated[str, Depends(oauth2_scheme)]) -> Optional[AgentModel]:
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
    async with agent_session(AgentModelOptional(id=token_data.id)) as session:
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


async def get_token(username, password, expiration_minutes=30):
    async with client_scoped_session() as session:
        token = await session.scalar(select(func.get_token(username, password, expiration_minutes)))
        if token:
            await session.commit()   # Updating last_login
        return token


CurrentAgentType = Annotated[AgentModel, Depends(get_current_agent)]
CurrentActiveAgentType = Annotated[AgentModel, Depends(get_current_active_agent)]

async def set_role(session, agent_name: str):
    db_name = str(session.bind.engine.url).split('/')[-1]
    await session.execute(text(f"SET ROLE {db_name}__{agent_name}"))

@asynccontextmanager
async def agent_session(agent: Optional[AgentModel]):
    async with client_scoped_session() as session:
        try:
            if agent:
                await set_role(session, f"m_{agent.id}")
            yield session
        finally:
            if agent:
                with suppress(Exception):
                    await session.rollback()
                    await set_role(session, "client")
                    await session.commit()

@asynccontextmanager
async def escalated_session(session):
    try:
        current_user = await session.scalar(text("select current_user"))
        current_user = current_user.split("__")[1]
        if current_user != "owner":
            await set_role(session, "owner")
        yield session
    finally:
        if current_user != "owner":
            with suppress(Exception):
                await set_role(session, current_user)
