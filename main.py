from fastapi import FastAPI
import uvicorn
from endpoints.transfer import router as transfer_router
from logger.gcp_logger import GCPLogger, LogLevel

app = FastAPI(title="BingAds -> BigQuery Transfer")

app.include_router(transfer_router)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}

# Run the application with Uvicorn when the script is executed directly
if __name__ == "__main__":
    GCPLogger.log(LogLevel.INFO, 'main',"Starting the Uvicorn server" )
    uvicorn.run("main:app", host="0.0.0.0", port=8000 , reload=True)