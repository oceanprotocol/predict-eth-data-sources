import uvicorn

from fastapi import FastAPI
from routers import data, health_check

app = FastAPI()

app.include_router(data.router)
app.include_router(health_check.router)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
