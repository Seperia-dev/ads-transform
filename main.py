import os
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from endpoints.transfer import router as transfer_router
from logger.gcp_logger import GCPLogger, LogLevel

app = FastAPI(title="AdsTransfer -> BigQuery")
load_dotenv()
ALLOWED_ORIGINS = os.getenv(
    'ALLOWED_ORIGINS',
    "http://localhost:3000",
).split(",")
PORT = int(os.getenv('PORT', 8000))
DEBUG = os.getenv('DEBUG', "False").lower() == "true"
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(transfer_router)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


if __name__ == "__main__":
    GCPLogger.log(LogLevel.INFO, "main", "Starting the Uvicorn server")
    uvicorn.run("main:app", host="0.0.0.0", port=PORT , reload=DEBUG)