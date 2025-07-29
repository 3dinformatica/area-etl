from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


def find_env_file() -> str:
    """Find .env file in current directory or parent directories."""
    current_dir = Path.cwd()
    env_file = current_dir / ".env"

    # Check the current directory
    if env_file.exists():
        return str(env_file)

    # Check parent directories (up to 3 levels)
    for _ in range(3):
        current_dir = current_dir.parent
        env_file = current_dir / ".env"
        if env_file.exists():
            return str(env_file)

    # Return the default path if not found
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
    ORACLE_URI_AREA: str
        Connection string for Oracle database for main A.Re.A. services
    ORACLE_URI_POA: str
        Connection string for Oracle database for POA A.Re.A. services
    PG_URI_CORE: str
        Connection string for PostgreSQL A.Re.A. Core service database
    PG_URI_POA: str
        Connection string for PostgreSQL A.Re.A. POA service database
    PG_URI_CRONOS: str
        Connection string for PostgreSQL A.Re.A. Cronos service database
    PG_URI_AUAC: str
        Connection string for PostgreSQL A.Re.A. Au.Ac. service database
    MINIO_ENDPOINT: str
        Endpoint URL for MinIO object storage
    MINIO_ACCESS_KEY: str
        Access key for MinIO object storage
    MINIO_SECRET_KEY: str
        Secret key for MinIO object storage
    ATTACHMENTS_DIR: str
        Directory for storing attachments
    """

    ORACLE_CLIENT_LIB_DIR: str = "/path/to/instantclient"
    ORACLE_URI_AREA: str = "oracle://username:password@hostname:1521/service_name_area"
    ORACLE_URI_POA: str = "oracle://username:password@hostname:1521/service_name_poa"
    PG_URI_CORE: str = "postgresql://username:password@hostname:5432/area_core_db"
    PG_URI_POA: str = "postgresql://username:password@hostname:5432/area_poa_db"
    PG_URI_CRONOS: str = "postgresql://username:password@hostname:5432/area_cronos_db"
    PG_URI_AUAC: str = "postgresql://username:password@hostname:5432/area_auac_db"
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    ATTACHMENTS_DIR: str = "attachments"

    model_config = SettingsConfigDict(
        env_file=find_env_file(),
        env_file_encoding="utf-8",
        case_sensitive=True,
    )


# Create a global instance of the settings
settings = Settings()
