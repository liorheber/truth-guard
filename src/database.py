from snowflake.core import Root
from snowflake.snowpark import Session
from src.config import SNOWFLAKE_CONFIG


DATABASE = "HISTORICAL_FACTS_DB"
SCHEMA = "PUBLIC"
CORTEX_SEARCH_SERVICE = "CHECK_HISTORICAL_FACTS"


def get_cortex_search_services(session):
    """Get cortex search function."""
    root = Root(session)
    return root.databases[DATABASE].schemas[SCHEMA].cortex_search_services[
        CORTEX_SEARCH_SERVICE]


def create_snowflake_session():
    """Create and return a Snowflake session."""
    return Session.builder.configs(SNOWFLAKE_CONFIG).create()


def init_database(session):
    """Initialize the database, schema, and required tables."""
    # Create database and schema
    session.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}").collect()
    session.sql(f"USE DATABASE {DATABASE}").collect()
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}").collect()
    session.sql(f"USE SCHEMA {SCHEMA}").collect()

    # Create document stage if it doesn't exist
    session.sql("""
    CREATE STAGE IF NOT EXISTS DOCUMENT_STAGE
        FILE_FORMAT = (TYPE='CSV')
        DIRECTORY = (ENABLE=TRUE)
        ENCRYPTION=(TYPE='SNOWFLAKE_SSE')
    """).collect()

    # Create main corpus table
    session.sql("""
    CREATE TABLE IF NOT EXISTS HOLOCAUST_CORPUS (
        ID VARCHAR,
        SOURCE_NAME VARCHAR,
        TEXT VARCHAR,
        CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """).collect()

    # Create embeddings table (recreate to ensure latest schema)
    session.sql("DROP TABLE IF EXISTS HOLOCAUST_CORPUS_EMBEDDINGS").collect()
    session.sql("""
    CREATE TABLE HOLOCAUST_CORPUS_EMBEDDINGS (
        ID VARCHAR,
        SOURCE_NAME VARCHAR,
        TEXT VARCHAR,
        EMBEDDING VARIANT,
        ORIGINAL_ID VARCHAR,
        CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """).collect()

def verify_cortex_access(session):
    """Verify access to required Cortex functions."""
    try:
        # List available functions in CORTEX schema
        functions = session.sql("""
        SHOW USER FUNCTIONS IN SCHEMA SNOWFLAKE.CORTEX;
        """).collect()
        
        print("Available Cortex functions:")
        for func in functions:
            print(f"- {func['name']}")
            
        # Test basic function access
        session.sql("""
        SELECT 1;
        """).collect()
        
        return True
    except Exception as e:
        print(f"Error verifying Cortex access: {str(e)}")
        return False
