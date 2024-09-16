from fastapi import APIRouter
from utils import add_route

from Api.AiReport.Api import createRequest

router = APIRouter()
add_route(router, createRequest)