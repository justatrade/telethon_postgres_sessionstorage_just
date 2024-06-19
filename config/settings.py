from pathlib import Path
from pydantic import BaseSettings
from pydantic.fields import Field


class Settings(BaseSettings):
    min_pool_connections: str = Field(1, env="min_pool_connections")
    max_pool_connections: str = Field(10, env="max_pool_connections")
    db_username: str = Field(..., env="DB_USERNAME")
    db_password: str = Field(..., env="DB_PASSWORD")
    db_host: str = Field("127.0.0.1", env="DB_HOST")
    db_port: int = Field(5432, env="DB_PORT")
    db_name: str = Field(..., env="DB_NAME")

    class Config:
        env_file: str = ".env"
        env_file_encoding: str = "utf-8"
