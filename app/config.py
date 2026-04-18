from pathlib import Path
from pydantic import BaseModel, Field
from dotenv import load_dotenv
import os


load_dotenv()


class Settings(BaseModel):
    openai_api_key: str = Field(..., alias="OPENAI_API_KEY")
    openai_model: str = Field(default="gpt-4o-mini", alias="OPENAI_MODEL")
    data_dir: Path = Field(default=Path("./app/data"), alias="DATA_DIR")
    db_path: Path = Field(default=Path("./app/storage/fraud_mvp.db"), alias="DB_PATH")


def get_settings() -> Settings:
    return Settings(
        OPENAI_API_KEY=os.getenv("OPENAI_API_KEY"),
        OPENAI_MODEL=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
        DATA_DIR=os.getenv("DATA_DIR", "./app/data"),
        DB_PATH=os.getenv("DB_PATH", "./app/storage/fraud_mvp.db"),
    )