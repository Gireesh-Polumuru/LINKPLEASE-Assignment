from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration loaded from environment variables or .env file."""

    APP_NAME: str = "LinkPlease"
    DEBUG: bool = False
    HOST: str = "0.0.0.0"
    PORT: int = 8000

    # Database Configuration (supports postgresql+asyncpg:// and sqlite+aiosqlite://)
    DATABASE_URL: str = "sqlite+aiosqlite:///./linkplease.db"

    # PseudoGram API Configuration
    PSEUDOGRAM_BASE_URL: str = "https://pseudogram-api.onrender.com"
    PSEUDOGRAM_API_KEY: str = ""

    # Webhook Security
    WEBHOOK_SECRET: str = ""
    VERIFY_WEBHOOK_SIGNATURE: bool = True

    # Rate Limiting: 10 sends per 60 seconds rolling window
    DM_SEND_RATE_LIMIT: int = 10
    DM_SEND_RATE_WINDOW_SECONDS: int = 60

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
