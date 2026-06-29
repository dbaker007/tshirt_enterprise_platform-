import os


def get_platform_database_url(port: str = "5432") -> str:
    """CENTRALIZED INFRASTRUCTURE FACTORY: Compiles a standardized

    connection URL from environment parameters or network defaults.
    """
    # Check for an explicit, offline testing protocol override variable first
    testing_override = os.getenv("PLATFORM_DATABASE_URL")
    if testing_override:
        return testing_override

    db_host = os.getenv("DATABASE_HOST", "localhost")
    safe_port = str(port) if port else "5432"

    return f"postgresql://platform_admin:admin_secure_password@{db_host}:{safe_port}/platform_shared_ledger"
