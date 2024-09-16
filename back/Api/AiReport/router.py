from fastapi import APIRouter

router = APIRouter(
    prefix="/api/aiReport",
    tags=["AiReport"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)