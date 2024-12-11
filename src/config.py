import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "HISTORICAL_FACTS_DB"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
}

# Validate required configuration
required_configs = [
    "account", "user", "password", "role", "warehouse"
]

missing_configs = [
    config for config in required_configs 
    if not SNOWFLAKE_CONFIG.get(config)
]

if missing_configs:
    raise ValueError(
        f"Missing required Snowflake configurations: {', '.join(missing_configs)}"
    )

# Create a .env template file if it doesn't exist
if not os.path.exists(".env"):
    with open(".env", "w") as f:
        f.write("""SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_WAREHOUSE=your_warehouse
""")
