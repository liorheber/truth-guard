from snowflake.core import Root
from snowflake.snowpark import Session
from src.config import SNOWFLAKE_CONFIG

DATABASE = "HISTORICAL_FACTS_DB"
SCHEMA = "PUBLIC"
VERIFIED_DOCS_CHUNKS = "VERIFIED_DOCS_CHUNKS"
VERIFIED_DOCS_SEARCH_SERVICE = "VERIFIED_DOCS_SEARCH_SERVICE"
UNVERIFIED_DOCS_SEARCH_SERVICE = "UNVERIFIED_DOCS_SEARCH_SERVICE"
VERIFIED_DOCUMENT_STAGE = "VERIFIED_DOCUMENT_STAGE"
UNVERIFIED_DOCUMENT_STAGE = "UNVERIFIED_DOCUMENT_STAGE"
UNVERIFIED_DOCS_CHUNKS = "UNVERIFIED_DOCS_CHUNKS"


def get_css(session):
    """Get cortex search function."""
    root = Root(session)
    return root.databases[DATABASE].schemas[SCHEMA].cortex_search_services[VERIFIED_DOCS_SEARCH_SERVICE]


def create_snowflake_session():
    """Create and return a Snowflake session."""
    return Session.builder.configs(SNOWFLAKE_CONFIG).create()


def init_database(session):
    """Initialize the database, schema, and required tables."""
    statuses = []
    # Create database and schema
    statuses.append(session.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}").collect())
    statuses.append(session.sql(f"USE DATABASE {DATABASE}").collect())
    statuses.append(session.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}").collect())
    statuses.append(session.sql(f"USE SCHEMA {SCHEMA}").collect())

    # Create document stage if it doesn't exist
    statuses.append(session.sql(f"""
    CREATE STAGE IF NOT EXISTS {VERIFIED_DOCUMENT_STAGE}
        FILE_FORMAT = (TYPE='CSV')
        DIRECTORY = (ENABLE=TRUE)
        ENCRYPTION=(TYPE='SNOWFLAKE_SSE')
    """).collect())

    # Create unverified document stage if it doesn't exist
    statuses.append(session.sql(f"""
    CREATE STAGE IF NOT EXISTS {UNVERIFIED_DOCUMENT_STAGE}
        FILE_FORMAT = (TYPE='CSV')
        DIRECTORY = (ENABLE=TRUE)
        ENCRYPTION=(TYPE='SNOWFLAKE_SSE')
    """).collect())

    # Create verified chunks table
    statuses.append(session.sql(f"""
    CREATE TABLE IF NOT EXISTS {VERIFIED_DOCS_CHUNKS} ( 
        RELATIVE_PATH VARCHAR(1000),
        SIZE NUMBER(38,0),
        FILE_URL VARCHAR(1000),
        SCOPED_FILE_URL VARCHAR(1000),
        CHUNK VARCHAR(16777216)
    );
    """).collect())

    # Create unverified chunks table
    statuses.append(session.sql(f"""
        CREATE TABLE IF NOT EXISTS {UNVERIFIED_DOCS_CHUNKS} (
            ID NUMBER(38,0) AUTOINCREMENT,
            RELATIVE_PATH VARCHAR(1000),
            SIZE NUMBER(38,0),
            FILE_URL VARCHAR(1000),
            SCOPED_FILE_URL VARCHAR(1000),
            CHUNK VARCHAR(16777216),
            STATEMENTS VARCHAR(16777216)
        );
            """).collect())

    # cleanup unverified stage and table - TODO: work only on our file and not delete everything
    docs = session.sql(f"list @{UNVERIFIED_DOCUMENT_STAGE}").collect()
    for doc in docs:
        print(f"Removing {doc.name}")
        session.sql(f"remove @{doc.name}").collect()
    session.sql(f"delete from {UNVERIFIED_DOCS_CHUNKS}").collect()

    # TODO: maybe we have to think about the check sizes and the overlap, and maybe define another chunker for the unverified documents
    # Create text chunker for verified documents
    statuses.append(session.sql(f"""
    create or replace function text_chunker(pdf_text string)
returns table (chunk varchar)
language python
runtime_version = '3.9'
handler = 'text_chunker'
packages = ('snowflake-snowpark-python', 'langchain')
as
$$
from snowflake.snowpark.types import StringType, StructField, StructType
from langchain.text_splitter import RecursiveCharacterTextSplitter
import pandas as pd

class text_chunker:

    def process(self, pdf_text: str):
        
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size = 1024,
            chunk_overlap  = 124, 
            length_function = len
        )
    
        chunks = text_splitter.split_text(pdf_text)
        df = pd.DataFrame(chunks, columns=['chunks'])
        
        yield from df.itertuples(index=False, name=None)
$$;
    """).collect())

    # Create cortex search for verified chunks table
    statuses.append(session.sql(f"""
    CREATE CORTEX SEARCH SERVICE IF NOT EXISTS {VERIFIED_DOCS_SEARCH_SERVICE}
    ON CHUNK
    WAREHOUSE = COMPUTE_WH
    TARGET_LAG = '1 minute'
    AS (
        SELECT CHUNK,
            RELATIVE_PATH,
            FILE_URL
        FROM {VERIFIED_DOCS_CHUNKS}
    );
    """).collect())
    print(statuses)
    return all([all(["successfully" in stat['status'] or "succeeded" in stat["status"] for stat in status]) for status in statuses])


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
