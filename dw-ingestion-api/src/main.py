from fastapi import FastAPI
from src.routers.ingestion import router
from src.config import get_settings

settings = get_settings()

app = FastAPI(
    title="DW Ingestion API",
    description="API for staging financial market data from DC team.",
    version="0.1.0",
)

app.include_router(router)
