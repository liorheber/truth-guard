import streamlit as st
from src.database import create_snowflake_session, init_database, verify_cortex_access


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
    st.header("Upload and Fact Check a Document")
    st.markdown("""
    Upload a PDF document to verify its claims against our trusted corpus.
    Documents that pass verification will be added to the corpus.
    """)

    uploaded_file = st.file_uploader(
        "Upload a PDF to fact-check:",
        type=["pdf"],
        help="The document will be analyzed for factual claims and verified against our trusted corpus."
    )

else:  # Ask a Question
    if "messages" not in st.session_state:
        st.session_state.messages = [
            {"role": "assistant", "content": "How may I help you?"}
        ]
    
    # Display messages
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Input at the bottom
    prompt = st.chat_input("Your message")
    if prompt:
        st.session_state.messages.append({"role": "user", "content": prompt})
        response = f"Echo: {prompt}"
        st.session_state.messages.append({"role": "assistant", "content": response})
        st.rerun()