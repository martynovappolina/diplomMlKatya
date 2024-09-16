import types
from fastapi import APIRouter
from yoyo import read_migrations, get_backend
from database import SQLALCHEMY_DATABASE_URL, SessionLocal


def run_migration():
    backend = get_backend(SQLALCHEMY_DATABASE_URL)
    migrations = read_migrations('./migrations')
    print('migrations.len: ' + str(len(migrations)))
    with backend.lock():
        backend.apply_migrations(backend.to_apply(migrations))

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


prefixes = []


def add_route(router_base: APIRouter, method: types.ModuleType):
    if method.router.prefix not in prefixes:
        router_base.include_router(method.router)
        prefixes.append(method.router.prefix)

