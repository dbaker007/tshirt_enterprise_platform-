import os


def get_platform_database_url(port: str = "5432") -> str:
    """CENTRALIZED INFRASTRUCTURE FACTORY: Compiles a standardized

    PostgreSQL connection URL from raw network parameters.
    """
    db_host = os.getenv("DATABASE_HOST", "localhost")
    safe_port = str(port) if port else "5432"

    return f"postgresql://platform_admin:admin_secure_password@{db_host}:{safe_port}/platform_shared_ledger"
