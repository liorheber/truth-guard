from initial_file_ingestion import upload_file_to_stage, write_file_to_stage, chunks_into_table
from src.database import UNVERIFIED_DOCUMENT_STAGE, UNVERIFIED_DOCS_CHUNKS



def create_statements(session):
    create_statements_sql = ("CREATE OR REPLACE TEMPORARY TABLE chunks_statements AS WITH unique_statement AS "
                             f"(SELECT DISTINCT chunk FROM {UNVERIFIED_DOCS_CHUNKS}), "
            "chunks_statements AS (SELECT relative_path, "
           "TRIM(snowflake.cortex.COMPLETE ('llama3-70b', "
           "'Create a list of statements documented in this text: ' || chunk || ), "
                             "'\n') AS statements "
           "FROM unique_statement) "
           "SELECT * FROM chunks_statements;"
            )
    print(create_statements_sql)
    session.sql(create_statements_sql).collect()
    update_table_sql = (f"update {UNVERIFIED_DOCS_CHUNKS}  "
           f"SET statements = chunks_statements.statements "
           f"from chunks_statements "
           f"where  {UNVERIFIED_DOCS_CHUNKS}.relative_path = chunks_statements.relative_path;")
    session.sql(update_table_sql).collect()

def verify_document(uploaded_file, session):
    # 1. upload to unverified stage
    upload_file_to_stage(session, uploaded_file, UNVERIFIED_DOCUMENT_STAGE)
    # 2. chunk the document into an unverified table
    chunks_into_table(session, UNVERIFIED_DOCUMENT_STAGE, UNVERIFIED_DOCS_CHUNKS)
    # 3. use cortex functions to create statements from each chunk
    create_statements(session)
    # 4. compare statements to verified corpus
    # 5. make a decision: if there are contradictions, reject the document, if not, accept it?


def verify_doc(st, session):
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

    if uploaded_file:
        st.write("Uploaded file:", uploaded_file.name)
        # save file locally
        temp_file_path = "tmp/"+uploaded_file.name
        with open(temp_file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())

        if st.button("Verify Document"):
            st.spinner("Verifying document...")
            verify_document(temp_file_path, session)



