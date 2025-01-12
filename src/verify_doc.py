import json
import os
import pandas as pd

from initial_file_ingestion import upload_file_to_stage, write_file_to_stage, chunks_into_table, refresh_stage
from src.chat import NUM_CHUNKS, COLUMNS
from src.database import *


class VerifyDoc:
    def __init__(self, streamlit, session, css, config):
        self.st = streamlit
        self.session = session
        self.css = css
        self.config = config
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
        search_results = self.css.search(statement, COLUMNS, limit=NUM_CHUNKS)
        print(f"Context for statement: {search_results}")
        
        # Extract the results from the search_results
        if hasattr(search_results, 'results'):
            context_list = search_results.results
        else:
            # If search_results is already a list
            context_list = search_results
        
        verify_prompt = f"""
            You are an expert chat assistance that verifies statements using the CONTEXT provided.
            If the statement is supported by the context, please answer "verified". If the statement is contradicted by the context, please answer "contradicted".
            If the statement is unrelated to the context, please answer "unverified".
            Do not add any additional words or context to the answer.
            <statement>{statement}</statement>
            <context>{context_list}</context>
            """
        cmd = "select snowflake.cortex.complete(?, ?) as response"
        df_response = self.session.sql(cmd, params=['mistral-large2', verify_prompt]).collect()
        verified = df_response[0].RESPONSE.strip()
        
        # Return the context in the format expected by display_statements
        formatted_context = []
        for ctx in context_list:
            if isinstance(ctx, dict):
                # If it's already a dictionary, use it as is
                formatted_context.append(ctx)
            else:
                # If it's a Row or other object, convert to dictionary
                formatted_context.append({
                    'relative_path': ctx.relative_path if hasattr(ctx, 'relative_path') else str(ctx),
                    'chunk': ctx.chunk if hasattr(ctx, 'chunk') else str(ctx)
                })
        
        return {
            'result': verified,
            'context': formatted_context
        }

    def create_chunk_score(self):
        # First, ensure the score column exists
        try:
            self.session.sql(f"ALTER TABLE {UNVERIFIED_DOCS_CHUNKS} ADD COLUMN IF NOT EXISTS score FLOAT").collect()
            
            # for each row in the unverified chunk table:
            get_statements_sql = f"SELECT * FROM {UNVERIFIED_DOCS_CHUNKS}"
            statements = self.session.sql(get_statements_sql).collect()
            total_statements = len(statements)
            
            progress_bar = self.st.progress(0, "Verifying statements")
            status_text = self.st.empty()
            results_area = self.st.container()
            
            # Store verification results
            if 'verification_results' not in self.st.session_state:
                self.st.session_state.verification_results = []
            
            total_verifications = 0
            total_verified = 0
            contradicted = 0
            unverified = 0
            
            for idx, statement in enumerate(statements):
                try:
                    status_text.write(f"Processing chunk {idx + 1} of {total_statements}")
                    
                    # Handle both string and None cases
                    statements_str = statement["STATEMENTS"] if "STATEMENTS" in statement.asDict() else None
                    if not statements_str:
                        continue
                    
                    # Clean up the statements string
                    statements_str = statements_str.strip()
                    if statements_str.startswith('```json'):
                        statements_str = statements_str[7:]
                    if statements_str.endswith('```'):
                        statements_str = statements_str[:-3]
                    statements_str = statements_str.strip()
                        
                    statements_list = json.loads(statements_str)
                    if not isinstance(statements_list, list):
                        continue
                        
                    verifications = []
                    verification_text = []
                    
                    for st in statements_list:
                        if not isinstance(st, str):
                            continue
                        verification = self.verify_statement(st)
                        verification_text.append({
                            "statement": st,
                            "result": verification['result'],
                            "context": verification['context']
                        })
                        verifications.append(verification['result'])
                        total_verifications += 1
                        if verification['result'].lower() == "verified":
                            total_verified += 1
                        elif verification['result'].lower() == "contradicted":
                            contradicted += 1
                        else:
                            unverified += 1
                    
                    # Store results for this chunk
                    if verification_text:
                        self.st.session_state.verification_results.append({
                            "chunk_num": idx + 1,
                            "verifications": verification_text
                        })
                    
                    if verifications:
                        score = sum([1 if v.lower() == "verified" else 0 for v in verifications]) / len(verifications)
                        update_score_sql = (f"UPDATE {UNVERIFIED_DOCS_CHUNKS} "
                                        f"SET score = {score} "
                                        f"WHERE id = {statement['ID']}")
                        self.session.sql(update_score_sql).collect()
                        
                    # Update progress
                    progress_bar.progress((idx + 1) / total_statements, 
                                       f"Processed {idx + 1} of {total_statements} chunks")
                        
                except Exception as e:
                    self.st.warning(f"Error processing chunk {idx + 1}: {str(e)}")
                    print(f"Detailed error for chunk {idx + 1}: {str(e)}")  # Terminal logging
                    continue
            
            # Show final analysis results
            status_text.empty()
            progress_bar.empty()
            
            if total_verifications > 0:
                with results_area:
                    verification_percentage = (total_verified / total_verifications) * 100
                    verification_stats = f"{total_verified} out of {total_verifications} statements verified"
                    
                    self.st.metric("Overall Verification Score", 
                                f"{verification_percentage:.1f}%",
                                verification_stats)
                    
                    # Display summary statistics
                    col1, col2, col3 = self.st.columns(3)
                    with col1:
                        self.st.metric("Verified Statements", total_verified)
                    with col2:
                        self.st.metric("Contradicted Statements", contradicted)
                    with col3:
                        self.st.metric("Unverified Statements", unverified)
                    
                    if total_verified == total_verifications:
                        self.st.success("✅ All statements verified successfully!")
                    else:
                        self.st.warning(f"⚠️ Some statements could not be verified or were contradicted.")
                    
                    # Show results table
                    self.st.subheader("Verification Details")
                    results_df = pd.DataFrame([
                        {"Result Type": "Verified", "Count": total_verified},
                        {"Result Type": "Contradicted", "Count": contradicted},
                        {"Result Type": "Unverified", "Count": unverified}
                    ])
                    self.st.bar_chart(results_df.set_index("Result Type"))
            
            return True
            
        except Exception as e:
            error_msg = f"Error in chunk analysis: {str(e)}"
            self.st.error(error_msg)
            print(f"Detailed error: {error_msg}")  # Terminal logging
            return False

    def display_verification_results(self):
        """Display the stored verification results"""
        if 'verification_results' in self.st.session_state and self.st.session_state.verification_results:
            # Display final status if available
            if 'verification_status' in self.st.session_state:
                if self.st.session_state.verification_status == "accepted":
                    self.st.success("## Document accepted and added to verified corpus 🎉")
                else:
                    self.st.error("## Document rejected 😔")
                    self.st.write("The document contains unverified or contradicted statements.")
            
            # Display final score if available
            if 'final_score' in self.st.session_state:
                self.st.metric(
                    "Verification Score", 
                    f"{self.st.session_state.final_score['percentage']:.1f}%", 
                    self.st.session_state.final_score['stats']
                )
            
            # Display detailed results
            self.st.subheader("Detailed Verification Results")
            
            # Create tabs for different verification statuses
            verified_statements = []
            contradicted_statements = []
            unverified_statements = []
            
            # Collect statements by verification status
            for chunk_result in self.st.session_state.verification_results:
                for v in chunk_result['verifications']:
                    result = v['result'].lower()
                    statement_info = {
                        'chunk': chunk_result['chunk_num'],
                        'statement': v['statement'],
                        'context': v['context']
                    }
                    if result == 'verified':
                        verified_statements.append(statement_info)
                    elif result == 'contradicted':
                        contradicted_statements.append(statement_info)
                    else:
                        unverified_statements.append(statement_info)
            
            # Create tabs
            tab1, tab2, tab3 = self.st.tabs(["✅ Verified", "❌ Contradicted", "❓ Unverified"])
            
            def display_statements(statements, status):
                if statements:
                    for stmt in statements:
                        self.st.markdown(f"### {stmt['statement']}")
                        self.st.markdown(f"**From Chunk {stmt['chunk']}**")
                        self.st.markdown("**Supporting Context:**")
                        for idx, context in enumerate(stmt['context'], 1):
                            self.st.markdown(f"""
                            ---
                            **Source {idx}**: {context['relative_path']}
                            ```
                            {context['chunk']}
                            ```
                            """)
                        self.st.divider()
                else:
                    self.st.info(f"No {status} statements found.")
            
            # Verified statements tab
            with tab1:
                display_statements(verified_statements, "verified")
            
            # Contradicted statements tab
            with tab2:
                display_statements(contradicted_statements, "contradicted")
            
            # Unverified statements tab
            with tab3:
                display_statements(unverified_statements, "unverified")
            
            # Show all statements in chronological order
            with self.st.expander("View All Statements in Order"):
                for chunk_result in self.st.session_state.verification_results:
                    self.st.markdown(f"### Chunk {chunk_result['chunk_num']}")
                    for v in chunk_result['verifications']:
                        status_emoji = "✅" if v['result'].lower() == 'verified' else "❌" if v['result'].lower() == 'contradicted' else "❓"
                        self.st.markdown(f"{status_emoji} **{v['result']}**: {v['statement']}")
                        self.st.markdown("**Supporting Context:**")
                        for idx, context in enumerate(v['context'], 1):
                            self.st.markdown(f"""
                            ---
                            **Source {idx}**: {context['relative_path']}
                            ```
                            {context['chunk']}
                            ```
                            """)
                        self.st.divider()
                    self.st.markdown("---")

    def verify_document(self, uploaded_file):
        results_placeholder = self.st.empty()
        
        with self.st.status("Processing document...") as status:
            # 1. upload to unverified stage
            status.update(label="Uploading document...")
            uploaded_files = upload_file_to_stage(self.session, uploaded_file, UNVERIFIED_DOCUMENT_STAGE)
            if not uploaded_files or len(uploaded_files) == 0:
                self.st.error("Error: Unable to upload the document")
                return
                
            # Refresh stage after upload
            status.update(label="Preparing document for analysis...")
            if not refresh_stage(self.session, UNVERIFIED_DOCUMENT_STAGE):
                self.st.error("Error: Unable to refresh stage after upload")
                return
                
            # 2. chunk the document
            status.update(label="Breaking document into analyzable chunks...")
            if not chunks_into_table(self.session, UNVERIFIED_DOCUMENT_STAGE, UNVERIFIED_DOCS_CHUNKS):
                self.st.error("Error: Unable to chunk the document")
                return
                
            # 3. create statements
            status.update(label="Extracting statements from chunks...")
            self.create_statements()
            
            # 4. verify statements
            status.update(label="Verifying statements against trusted corpus...")
            if not self.create_chunk_score():
                return
                
            # 5. make decision
            status.update(label="Making final verification decision...")
            num_verified = self.session.sql(
                f"SELECT COUNT(*) FROM {UNVERIFIED_DOCS_CHUNKS} WHERE score >= {self.config['verified_chunk_threshold']}").collect()[
                0]["COUNT(*)"]
            overall_chunk_length = self.session.sql(f"SELECT COUNT(*) FROM {UNVERIFIED_DOCS_CHUNKS}").collect()[0][
                "COUNT(*)"]

            verification_stats = f"{num_verified} out of {overall_chunk_length} chunks verified"
            verification_percentage = (num_verified / overall_chunk_length) * 100 if overall_chunk_length > 0 else 0
            
            # Store the final results in session state
            self.st.session_state.final_score = {
                "percentage": verification_percentage,
                "stats": verification_stats
            }

            accepted = num_verified >= overall_chunk_length * self.config['verified_chunks_percent_per_document']
            if accepted:
                status.update(label="Document accepted! Adding to verified corpus...", state="complete")
                self.st.session_state.verification_status = "accepted"
                
                # 6. if accepted, move to verified corpus
                write_file_to_stage(self.session, uploaded_file, VERIFIED_DOCUMENT_STAGE)
                if not refresh_stage(self.session, VERIFIED_DOCUMENT_STAGE):
                    self.st.error("Error: Unable to refresh verified stage after upload")
                    return
                chunks_into_table(self.session, VERIFIED_DOCUMENT_STAGE, VERIFIED_DOCS_CHUNKS)
            else:
                status.update(label="Document verification complete", state="complete")
                self.st.session_state.verification_status = "rejected"
                
            # 7. cleanup database and files
            self.session.sql(f"DELETE FROM {UNVERIFIED_DOCS_CHUNKS}").collect()
            self.session.sql(f"REMOVE @{UNVERIFIED_DOCUMENT_STAGE}/{uploaded_file}").collect()
            self.cleanup(uploaded_file, rerun=False)
            
            # Display results immediately
            with results_placeholder:
                self.display_verification_results()
            
            # Reset processing state
            self.st.session_state.processing = False

    def cleanup(self, uploaded_file, rerun=False):
        """Cleanup temporary files and optionally reset the UI"""
        os.remove(uploaded_file)
        # Clear session state for new upload
        self.st.session_state.current_file = None
        self.st.session_state.processing = False
        if rerun:
            self.st.rerun()

    def verify_doc(self):
        self.st.header("Upload and Fact Check a Document")
        self.st.markdown("""
        Upload a PDF document to verify its claims against our trusted corpus.
        Documents that pass verification will be added to the corpus.
        """)

        # Initialize session state
        if 'processing' not in self.st.session_state:
            self.st.session_state.processing = False
        if 'current_file' not in self.st.session_state:
            self.st.session_state.current_file = None

        # Create a placeholder for results
        results_area = self.st.empty()

        # Display existing results if available and not processing
        if not self.st.session_state.processing:
            with results_area:
                self.display_verification_results()

        uploaded_file = self.st.file_uploader(
            "Upload a PDF to fact-check:",
            type=["pdf"],
            help="The document will be analyzed for factual claims and verified against our trusted corpus.",
            disabled=self.st.session_state.processing
        )

        if uploaded_file:
            self.st.write("Uploaded file:", uploaded_file.name)
            # save file locally
            temp_file_path = "tmp/" + uploaded_file.name.replace(" ", "_")
            
            # Only write file if it's a new one
            if uploaded_file.name != self.st.session_state.current_file:
                with open(temp_file_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                self.st.session_state.current_file = uploaded_file.name
                # Clear previous results when new file is uploaded
                for key in ['verification_results', 'verification_status', 'final_score']:
                    if key in self.st.session_state:
                        del self.st.session_state[key]
                # Clear the results area
                results_area.empty()

            verify_button = self.st.button(
                "Verify Document",
                disabled=self.st.session_state.processing
            )

            if verify_button and not self.st.session_state.processing:
                self.st.session_state.processing = True
                # Clear previous results before starting new verification
                results_area.empty()
                self.verify_document(temp_file_path)
