from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from src.api.routes import router
import os

app = FastAPI(title="Poverty NYC API")
origins = os.getenv("ALLOWED_ORIGINS", "http://localhost:5173").split(",")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Enable Gzip Compression (minimum size 1000 bytes)
app.add_middleware(GZipMiddleware, minimum_size=1000)

app.include_router(router, prefix="/api")

@app.get("/")
async def root():
    return {"message": "Poverty NYC API"}
