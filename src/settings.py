from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Oracle client settings
    ORACLE_CLIENT_LIB_DIR: str

    # Database connection strings
    ORACLE_URI: str
    PG_URI: str

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )


# Create a global instance of the settings
settings = Settings()
