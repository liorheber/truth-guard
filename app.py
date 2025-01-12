import json

import streamlit as st

from src.chat import Chat
from src.database import *
from src.verify_doc import VerifyDoc


# Initialize Snowflake connection
@st.cache_resource
def init_snowflake():
    se = create_snowflake_session()
    if not verify_cortex_access(se):
        st.error("Error: Unable to access required Cortex functions")
        st.stop()
    if not init_database(se):
        st.error("Error: Unable to initialize the database")
        st.stop()
    return se

config = json.load(open("config.json"))

# Page config
st.set_page_config(
    page_title="Truth Guard",
    page_icon="🛡️",
    layout="wide"
)

# Initialize session
session = init_snowflake()
css_verified = get_css(session)


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
    VerifyDoc(st, session, css_verified, config).verify_doc()

else:  # Ask a Question
    Chat(st, session, css_verified).chat()