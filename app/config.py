from pydantic import BaseSettings

class Settings(BaseSettings):
    app_name: str = "Exchange data provider"
    transpose_api_key: str = ""

    class Config:
        env_file = ".env"

settings = Settings()
