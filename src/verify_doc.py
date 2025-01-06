import json
import os

from initial_file_ingestion import upload_file_to_stage, write_file_to_stage, chunks_into_table
from src.chat import NUM_CHUNKS, COLUMNS
from src.database import *


class VerifyDoc:
    def __init__(self, streamlit, session, css):
        self.st = streamlit
        self.session = session
        self.css = css
        os.makedirs("tmp/split_files", exist_ok=True)

    def create_statements(self):
        create_statements_sql = ("CREATE OR REPLACE TEMPORARY TABLE chunks_statements AS "
                                 "WITH unique_statement AS "
                                 f"(SELECT DISTINCT id, relative_path, chunk FROM {UNVERIFIED_DOCS_CHUNKS}), "
                                 "chunks_statements AS (SELECT id, relative_path, "
                                 "TRIM(snowflake.cortex.COMPLETE ('mistral-large2', "
                                 "'Return a json formatted list of statements documented in the text. "
                                 "Return only the list with no additional information."
                                 " <text>' || chunk || '</text>'), "
                                 "'\n') AS statements "
                                 "FROM unique_statement) "
                                 "SELECT * FROM chunks_statements;"
                                 )
        print(create_statements_sql)
        self.session.sql(create_statements_sql).collect()
        update_table_sql = (f"update {UNVERIFIED_DOCS_CHUNKS}  "
                            f"SET statements = chunks_statements.statements "
                            f"from chunks_statements "
                            f"where  {UNVERIFIED_DOCS_CHUNKS}.id = chunks_statements.id;")
        print(update_table_sql)
        self.session.sql(update_table_sql).collect()

    def verify_statement(self, statement):
        print(f"Finding context for statement: {statement}")
        statement_context = self.css.search(statement, COLUMNS, limit=NUM_CHUNKS)
        print(f"Context for statement: {statement_context}")
        verify_prompt = f"""
            You are an expert chat assistance that verifies statements using the CONTEXT provided.
            If the statement is supported by the context, please answer "verified". If the statement is contradicted by the context, please answer "contradicted".
            If the statement is unrelated to the context, please answer "unverified".
            Do not add any additional words or context to the answer.
            <statement>{statement}</statement>
            <context>{statement_context}</context>
            """
        cmd = "select snowflake.cortex.complete(?, ?) as response"
        df_response = self.session.sql(cmd, params=['mistral-large', verify_prompt]).collect()
        verified = df_response[0].RESPONSE.strip()
        return verified

    def create_chunk_score(self):
        # for each row in the unverified chunk table:
        get_statements_sql = f"SELECT * FROM {UNVERIFIED_DOCS_CHUNKS}"
        statements = self.session.sql(get_statements_sql).collect()
        for statement in statements:
            print(f"Statements from LLM: {statement}")
            try:
                statements = json.loads(statement["STATEMENTS"])
                verifications = []
                print(f"Statements to verify: {statements}")
                for st in statements:
                    print(f"Asking LLM regrading statement: {st}")
                    llm_answer = self.verify_statement(st)
                    print(f"LLM answer: {llm_answer}")
                    verifications.append(llm_answer)
                score = sum([1 if v.lower() == "verified" else 0 for v in verifications]) / len(verifications)
                update_score_sql = (f"update {UNVERIFIED_DOCS_CHUNKS} "
                                        f"SET score = {score} "
                                        f"where id = {statement['ID']}")
                self.session.sql(update_score_sql).collect()
            except json.JSONDecodeError:
                print("Error decoding JSON")
                continue

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
        num_verified = self.session.sql(f"SELECT COUNT(*) FROM {UNVERIFIED_DOCS_CHUNKS} WHERE score >= 0.9").collect()[0]["COUNT(*)"]
        overall_chunk_length = self.session.sql(f"SELECT COUNT(*) FROM {UNVERIFIED_DOCS_CHUNKS}").collect()[0]["COUNT(*)"]
        print(f"{num_verified} out of {overall_chunk_length} chunks have score >= 0.9")
        accepted = False
        if num_verified == overall_chunk_length:
            accepted = True
            print("Document accepted")
        else:
            print("Document rejected")
        # 6. if accepted, move to verified corpus, chunk and add to verified corpus, then delete from unverified corpus
        if accepted:
            write_file_to_stage(self.session, uploaded_file, VERIFIED_DOCUMENT_STAGE)
            chunks_into_table(self.session, VERIFIED_DOCUMENT_STAGE, VERIFIED_DOCS_CHUNKS)
        # 7. delete from unverified corpus
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
            temp_file_path = "tmp/" + uploaded_file.name.replace(" ", "_")
            with open(temp_file_path, "wb") as f:
                f.write(uploaded_file.getbuffer())

            if self.st.button("Verify Document"):
                self.st.spinner("Verifying document...")
                self.verify_document(temp_file_path)
