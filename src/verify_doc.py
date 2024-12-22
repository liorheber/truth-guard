from initial_file_ingestion import upload_file_to_stage, write_file_to_stage, chunks_into_table
from src.database import *


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

def create_chunk_score(session):
    # for each row in the unverified chunk table:
    # 1. get the statements
    # 2. for each statement, get the supporting evidence from the verified corpus using the search service
    # 3. calculate the score for each statement
    # 4. calculate the overall score for the chunk by doing an average of the scores of the statements
    # 5. update the chunk table with the score
    return

def verify_document(uploaded_file, session):
    # 1. upload to unverified stage
    upload_file_to_stage(session, uploaded_file, UNVERIFIED_DOCUMENT_STAGE)
    # 2. chunk the document into an unverified table
    chunks_into_table(session, UNVERIFIED_DOCUMENT_STAGE, UNVERIFIED_DOCS_CHUNKS)
    # 3. use cortex functions to create statements from each chunk
    create_statements(session)
    # 4. compare statements to verified corpus
    # go over all statements, and ask the RAG to find supporting evidence from the facts, and give a score to each statement
    create_chunk_score(session)
    # 5. make a decision: if there are contradictions, reject the document, if not, accept it?
    verified = session.sql(f"SELECT * FROM {UNVERIFIED_DOCS_CHUNKS} WHERE score > 0.8").collect()
    overall_chunk_length = session.sql(f"SELECT COUNT(*) FROM {UNVERIFIED_DOCS_CHUNKS}").collect()
    print(f"{verified} out of {overall_chunk_length} chunks have score > 0.8")
    accepted = False
    if len(verified) > overall_chunk_length*0.8:
        accepted = True
        print("Document accepted")
    else:
        print("Document rejected")
    # 6. if accepted, move to verified corpus, chunk and add to verified corpus, then delete from unverified corpus
    if accepted:
        write_file_to_stage(session, uploaded_file, VERIFIED_DOCUMENT_STAGE)
        chunks_into_table(session, VERIFIED_DOCUMENT_STAGE, VERIFIED_DOCS_CHUNKS)
        session.sql(f"DELETE FROM {UNVERIFIED_DOCS_CHUNKS}").collect()
        session.sql(f"REMOVE @{UNVERIFIED_DOCUMENT_STAGE}/{uploaded_file}").collect()
    # 7. if rejected, delete from unverified corpus
    else:
        session.sql(f"DELETE FROM {UNVERIFIED_DOCS_CHUNKS}").collect()
        session.sql(f"REMOVE @{UNVERIFIED_DOCUMENT_STAGE}/{uploaded_file}").collect()


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



