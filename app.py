import json
import random

import streamlit as st

from src.chat import chat
from src.database import create_snowflake_session, init_database, verify_cortex_access, get_cortex_search_services
from snowflake.core import Root

from src.verify_doc import verify_doc


# Initialize Snowflake connection
@st.cache_resource
def init_snowflake():
    session = create_snowflake_session()
    if not verify_cortex_access(session):
        st.error("Error: Unable to access required Cortex functions")
        st.stop()
    init_database(session)
    return session


# Page config
st.set_page_config(
    page_title="Truth Guard",
    page_icon="🛡️",
    layout="wide"
)

# Initialize session
session = init_snowflake()
svc = get_cortex_search_services(session)


# Sidebar
with st.sidebar:
    st.title("🛡️ Truth Guard")
    st.markdown("""
    This system helps verify documents against a trusted corpus and provides
    a Q&A interface for historical fact-checking.
    """)

    page = st.radio("Choose a task:", [
        "📄 Add & Verify Document",
        "❓ Ask a Question"
    ])

# Main content
if page == "📄 Add & Verify Document":
    verify_doc(st)

else:  # Ask a Question
    chat(st, session, svc)