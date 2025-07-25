from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


def find_env_file() -> str:
    """Find .env file in current directory or parent directories."""
    current_dir = Path.cwd()
    env_file = current_dir / ".env"

    # Check current directory
    if env_file.exists():
        return str(env_file)

    # Check parent directories (up to 3 levels)
    for _ in range(3):
        current_dir = current_dir.parent
        env_file = current_dir / ".env"
        if env_file.exists():
            return str(env_file)

    # Return default path if not found
    return ".env"


class Settings(BaseSettings):
    """
    Application configuration settings loaded from environment variables or .env file.

    This class defines the configuration settings for the ETL process, including
    database connection strings and file paths. Settings can be loaded from
    environment variables or a .env file.

    Attributes
    ----------
    ORACLE_CLIENT_LIB_DIR: str
        Path to Oracle Instant Client directory
    ORACLE_URI: str
        Connection string for Oracle database
    PG_URI: str
        Connection string for PostgreSQL database
    PG_TABLE_PREFIX: str
        Prefix for PostgreSQL tables
    ATTACHMENTS_DIR: str
        Directory for storing attachments
    """

    ORACLE_CLIENT_LIB_DIR: str = "/path/to/instantclient"
    ORACLE_URI: str = "oracle://username:password@hostname:1521/service_name"
    PG_URI: str = "postgresql://username:password@localhost:5432/database_name"
    PG_TABLE_PREFIX: str = ""
    ATTACHMENTS_DIR: str = "attachments"

    model_config = SettingsConfigDict(
        env_file=find_env_file(),
        env_file_encoding="utf-8",
        case_sensitive=True,
    )


# Create a global instance of the settings
settings = Settings()
