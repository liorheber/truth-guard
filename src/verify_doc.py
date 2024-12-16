def verify_doc(st):
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