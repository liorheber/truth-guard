from snowflake.core import Root
from snowflake.snowpark import Session
from src.config import SNOWFLAKE_CONFIG


DATABASE = "HISTORICAL_FACTS_DB"
SCHEMA = "PUBLIC"
VERIFIED_DOCS_CHUNKS = "VERIFIED_DOCS_CHUNKS"
VERIFIED_DOCS_SEARCH_SERVICE = "VERIFIED_DOCS_SEARCH_SERVICE"
VERIFIED_DOCUMENT_STAGE = "VERIFIED_DOCUMENT_STAGE"
UNVERIFIED_DOCUMENT_STAGE = "UNVERIFIED_DOCUMENT_STAGE"
UNVERIFIED_DOCS_CHUNKS = "UNVERIFIED_DOCS_CHUNKS"


def get_cortex_search_services(session):
    """Get cortex search function."""
    root = Root(session)
    return root.databases[DATABASE].schemas[SCHEMA].cortex_search_services[VERIFIED_DOCS_SEARCH_SERVICE]


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
    session.sql(f"""
    CREATE STAGE IF NOT EXISTS {VERIFIED_DOCUMENT_STAGE}
        FILE_FORMAT = (TYPE='CSV')
        DIRECTORY = (ENABLE=TRUE)
        ENCRYPTION=(TYPE='SNOWFLAKE_SSE')
    """).collect()

    # Create document stage if it doesn't exist
    session.sql(f"""
    CREATE STAGE IF NOT EXISTS {UNVERIFIED_DOCUMENT_STAGE}
        FILE_FORMAT = (TYPE='CSV')
        DIRECTORY = (ENABLE=TRUE)
        ENCRYPTION=(TYPE='SNOWFLAKE_SSE')
    """).collect()

    # Create verified chunks table
    session.sql(f"""
    CREATE TABLE IF NOT EXISTS {VERIFIED_DOCS_CHUNKS} ( 
        RELATIVE_PATH VARCHAR(16777216),
        SIZE NUMBER(38,0),
        FILE_URL VARCHAR(16777216),
        SCOPED_FILE_URL VARCHAR(16777216),
        CHUNK VARCHAR(16777216),
        CATEGORY VARCHAR(16777216)
    );
    """).collect()

# TODO: maybe we have to think about the check sizes and the overlap, and maybe define another chunker for the unverified documents
    # Create text chunker for verified documents
    session.sql(f"""
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
            chunk_size = 1512, #Adjust this as you see fit
            chunk_overlap  = 256, #This let's text have some form of overlap. Useful for keeping chunks contextual
            length_function = len
        )
    
        chunks = text_splitter.split_text(pdf_text)
        df = pd.DataFrame(chunks, columns=['chunks'])
        
        yield from df.itertuples(index=False, name=None)
$$;
    """).collect()

    # Create cortex search for verified chunks table
    session.sql(f"""
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
    """).collect()

    session.sql(f"""
    CREATE TABLE IF NOT EXISTS {UNVERIFIED_DOCS_CHUNKS} (
        RELATIVE_PATH VARCHAR(16777216),
        SIZE NUMBER(38,0),
        FILE_URL VARCHAR(16777216),
        SCOPED_FILE_URL VARCHAR(16777216),
        CHUNK VARCHAR(16777216),
        STATEMENTS VARCHAR(16777216)
    );
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

