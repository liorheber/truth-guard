# TruthGuard
Dynamic Fact-Checking and Knowledge Expansion on Historical Data

Participant of [Snowflake Hackatho](https://snowflake-mistral-rag.devpost.com/)

## High-Level Architecture

1. **Snowflake Setup**:
   - Create `HISTORICAL_FACTS_DB`, `PUBLIC` schema, and two tables: `HOLOCAUST_CORPUS` and `HOLOCAUST_CORPUS_EMBEDDINGS`.
   - Ensure a Snowflake internal stage `DOCUMENT_STAGE` for uploading PDFs.
   - Ensure `CORTEX_USER` role access for LLM and embedding functions.

2. **Local PDFs Bulk Ingestion Script**:
   - Takes a local `documents/` directory.
   - Uploads each PDF to Snowflake stage.
   - Uses `PARSE_DOCUMENT` to extract text.
   - Cleans, chunks, verifies claims, and if supported, stores embeddings.

3. **Interactive Upload & Misinformation Check (Streamlit)**:
   - A “Verify & Add Document” tab in the Streamlit app:
     - User uploads a PDF.
     - App uploads it to Snowflake stage, parses, and extracts claims.
     - Verifies claims using the existing corpus.
     - If claims are supported, it inserts and embeds. If contradicted, notifies user and discards.
     - This allows the user to test the misinformation detection workflow in real-time.

4. **Q&A (Streamlit)**:
   - A “Ask a Question” tab to input queries.
   - Retrieves top relevant chunks and uses `COMPLETE` to provide a factual, sourced answer.

5. **Cortex Functions**:
   - `PARSE_DOCUMENT`: Extract text from PDFs on the stage.
   - `EMBED_TEXT_768`: Create embeddings for chunked text.
   - `COMPLETE`: With `mistral-large2` or another LLM to extract claims, verify them, and answer queries.

6. **Section-Based Chunking & Claim Verification**:
   - Smart chunking based on section headers or fallback token limits.
   - Claim extraction and verification ensure only truthful documents enter the corpus.
