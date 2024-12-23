from snowflake import snowpark
from snowflake.snowpark.functions import udf, col

from initial_file_ingestion import upload_file_to_stage, write_file_to_stage, chunks_into_table
from src.chat import NUM_CHUNKS, COLUMNS
from src.database import *


class VerifyDoc:
    def __init__(self, streamlit, session, svc):
        self.st = streamlit
        self.session = session
        self.svc = svc

    def create_statements(self):
        create_statements_sql = ("CREATE OR REPLACE TEMPORARY TABLE chunks_statements AS "
                                 "WITH unique_statement AS "
                                 f"(SELECT DISTINCT relative_path, chunk FROM {UNVERIFIED_DOCS_CHUNKS}), "
                                 "chunks_statements AS (SELECT relative_path, "
                                 "TRIM(snowflake.cortex.COMPLETE ('llama3-70b', 'Create a json formatted list of statements documented in this text: ' || chunk), "
                                 "'\n') AS statements "
                                 "FROM unique_statement) "
                                 "SELECT * FROM chunks_statements;"
                                 )
        print(create_statements_sql)
        self.session.sql(create_statements_sql).collect()
        update_table_sql = (f"update {UNVERIFIED_DOCS_CHUNKS}  "
                            f"SET statements = chunks_statements.statements "
                            f"from chunks_statements "
                            f"where  {UNVERIFIED_DOCS_CHUNKS}.relative_path = chunks_statements.relative_path;")
        self.session.sql(update_table_sql).collect()

    def verify_statement(self, statement):
        statement_context = self.svc.search(statement, COLUMNS, limit=NUM_CHUNKS)
        verify_prompt = f"""
            You are an expert chat assistance that verifies statements using the CONTEXT provided.
            If the statement is supported by the context, please answer "verified". If the statement is contradicted by the context, please answer "contradicted".
            If the statement is unrelated to the context, please answer "unverified".
            <statement>{statement}</statement>
            <context>{statement_context}</context>
            """
        cmd = "select snowflake.cortex.complete(?, ?) as response"
        df_response = self.session.sql(cmd, params=['mistral-large', verify_prompt]).collect()
        verified = df_response[0].RESPONSE
        return verified

    def create_chunk_score(self):
        # for each row in the unverified chunk table:
        # 1. create table with statements from the statements list
        statements_df = self.session.table(UNVERIFIED_DOCS_CHUNKS)
        flattened_df = statements_df.with_column("statement", snowpark.functions.flatten(col(["statements"]))).drop("statements")
        flattened_df.show()

        # 2. for each statement, get the supporting evidence from the verified corpus using the search service
        verify_udf = udf(self.verify_statement, name="verify_statement", session=self.session)
        df_with_verified_tag = flattened_df.select(flattened_df["id"],
                                                   verify_udf(flattened_df["statement"]).alias("verified"))
        df_with_verified_tag.show()
        df_with_verified_tag.write.format("snowflake").option("dbtable", "chunks_statements_flattened").mode(
            "overwrite").save()

        # 4. calculate the overall score for the chunk by doing an average of the scores of the statements
        # 5. update the chunk table with the score
        print("TODO: implement create_chunk_score")
        return

    def verify_document(self, uploaded_file):
        # 1. upload to unverified stage
        upload_file_to_stage(self.session, uploaded_file, UNVERIFIED_DOCUMENT_STAGE)
        # 2. chunk the document into an unverified table
        chunks_into_table(self.session, UNVERIFIED_DOCUMENT_STAGE, UNVERIFIED_DOCS_CHUNKS)
        # 3. use cortex functions to create statements from each chunk
        self.create_statements()
        # 4. compare statements to verified corpus
        # go over all statements, and ask the RAG to find supporting evidence from the facts, and give a score to each statement
        self.create_chunk_score()
        # 5. make a decision: if there are contradictions, reject the document, if not, accept it?
        verified = self.session.sql(f"SELECT * FROM {UNVERIFIED_DOCS_CHUNKS} WHERE score > 0.8").collect()
        overall_chunk_length = self.session.sql(f"SELECT COUNT(*) FROM {UNVERIFIED_DOCS_CHUNKS}").collect()
        print(f"{verified} out of {overall_chunk_length} chunks have score > 0.8")
        accepted = False
        if len(verified) > overall_chunk_length * 0.8:
            accepted = True
            print("Document accepted")
        else:
            print("Document rejected")
        # 6. if accepted, move to verified corpus, chunk and add to verified corpus, then delete from unverified corpus
        if accepted:
            write_file_to_stage(self.session, uploaded_file, VERIFIED_DOCUMENT_STAGE)
            chunks_into_table(self.session, VERIFIED_DOCUMENT_STAGE, VERIFIED_DOCS_CHUNKS)
            self.session.sql(f"DELETE FROM {UNVERIFIED_DOCS_CHUNKS}").collect()
            self.session.sql(f"REMOVE @{UNVERIFIED_DOCUMENT_STAGE}/{uploaded_file}").collect()
        # 7. if rejected, delete from unverified corpus
        else:
            self.session.sql(f"DELETE FROM {UNVERIFIED_DOCS_CHUNKS}").collect()
            self.session.sql(f"REMOVE @{UNVERIFIED_DOCUMENT_STAGE}/{uploaded_file}").collect()

    def verify_doc(self):
        self.st.header("Upload and Fact Check a Document")
        self.st.markdown("""
        Upload a PDF document to verify its claims against our trusted corpus.
        Documents that pass verification will be added to the corpus.
        """)

        uploaded_file = self.st.file_uploader(
            "Upload a PDF to fact-check:",
            type=["pdf"],
            help="The document will be analyzed for factual claims and verified against our trusted corpus."
        )

        if uploaded_file:
            self.st.write("Uploaded file:", uploaded_file.name)
            # save file locally
            temp_file_path = "tmp/" + uploaded_file.name
            with open(temp_file_path, "wb") as f:
                f.write(uploaded_file.getbuffer())

            if self.st.button("Verify Document"):
                self.st.spinner("Verifying document...")
                self.verify_document(temp_file_path)
