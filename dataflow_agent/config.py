from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from pydantic import BaseModel, Field
from pydantic import model_validator

# Load .env from the project root (or wherever the user runs the CLI)
load_dotenv()


class Config(BaseModel):
    gemini_api_key: str = Field(default="")
    gemini_model: str = Field(default="gemini-2.5-flash")
    postgres_url: str | None = Field(default=None)
    snowflake_account: str | None = Field(default=None)
    snowflake_user: str | None = Field(default=None)
    snowflake_password: str | None = Field(default=None)
    snowflake_database: str | None = Field(default=None)
    snowflake_schema: str | None = Field(default=None)
    snowflake_warehouse: str | None = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def load_from_env(cls, values: dict) -> dict:
        values.setdefault("gemini_api_key", os.getenv("GEMINI_API_KEY", ""))
        values.setdefault("gemini_model", os.getenv("GEMINI_MODEL", "gemini-2.5-flash"))
        values.setdefault("postgres_url", os.getenv("POSTGRES_URL"))
        values.setdefault("snowflake_account", os.getenv("SNOWFLAKE_ACCOUNT"))
        values.setdefault("snowflake_user", os.getenv("SNOWFLAKE_USER"))
        values.setdefault("snowflake_password", os.getenv("SNOWFLAKE_PASSWORD"))
        values.setdefault("snowflake_database", os.getenv("SNOWFLAKE_DATABASE"))
        values.setdefault("snowflake_schema", os.getenv("SNOWFLAKE_SCHEMA"))
        values.setdefault("snowflake_warehouse", os.getenv("SNOWFLAKE_WAREHOUSE"))
        return values

    def require_gemini_key(self) -> None:
        if not self.gemini_api_key:
            raise ValueError(
                "GEMINI_API_KEY is not set. "
                "Copy .env.example to .env and add your key."
            )


config = Config()
