import os

from database import SessionLocal, SQLALCHEMY_DATABASE_URL
from Api.routers import router
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request, Response
import uvicorn
from utils import run_migration


app = FastAPI(title="Diplom API")

app.include_router(router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def db_session_middleware(request: Request, call_next):
    response = Response("Internal server error", status_code=500)
    try:
        request.state.db = SessionLocal()
        response = await call_next(request)
    finally:
        request.state.db.close()
    return response


if __name__ == "__main__":
    run_migration()
    uvicorn.run(app, port=8000)
