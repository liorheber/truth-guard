import json

from src.database import VERIFIED_DOCUMENT_STAGE

NUM_CHUNKS = 3  # Num-chunks provided as context. Play with this to check how it affects your accuracy

# columns to query in the service
COLUMNS = [
    "chunk",
    "relative_path",
]


class Chat:
    def __init__(self, streamlit, session, svc):
        self.st = streamlit
        self.session = session
        self.svc = svc

    def chat(self):
        if "messages" not in self.st.session_state:
            self.st.session_state.messages = [
                {"role": "assistant", "content": "How may I help you?"}
            ]
        
        if "related_documents" not in self.st.session_state:
            self.st.session_state.related_documents = []

        # Display messages
        for message in self.st.session_state.messages:
            with self.st.chat_message(message["role"]):
                self.st.markdown(message["content"])

        # Display related documents in sidebar
        if self.st.session_state.related_documents:
            self.st.sidebar.title("Related Documents")
            for doc in self.st.session_state.related_documents:
                self.st.sidebar.markdown(doc)

        # Input at the bottom
        prompt = self.st.chat_input("Your message")
        if prompt:
            # Immediately show user message
            self.st.session_state.messages.append({"role": "user", "content": prompt})
            with self.st.chat_message("user"):
                self.st.markdown(prompt)

            # Show assistant message with loading state
            with self.st.chat_message("assistant"):
                with self.st.status("Thinking...", expanded=True) as status:
                    print(f"Got prompt: {prompt}")
                    if len(self.st.session_state.messages) > 1:
                        status.write("Analyzing conversation context...")
                        print(f"This is a continuation of the previous conversation, asking LLM to rephrase the question")
                        rephrase_prompt = (f"This is a chat history: "
                                       f"<chat> {' '.join([msg['content'] for msg in self.st.session_state.messages])} </chat> "
                                       f"Use this history to rephrase the following question. "
                                       f"rephrase the last question to contain all the necessary information needed to answer it. "
                                       f"Don't include unnecessary information. Phrase as a new question. "
                                       f"<question> {prompt} </question>")
                        print(f"Rephrasing prompt: {rephrase_prompt}")
                        cmd = """
                           select snowflake.cortex.complete(?, ?) as response
                        """
                        df_response = self.session.sql(cmd, params=['mistral-large', rephrase_prompt]).collect()
                        rephrased_question = df_response[0].RESPONSE
                        print(f"Rephrased question: {rephrased_question}")
                    else:
                        rephrased_question = prompt

                    status.write("Searching for relevant information...")
                    print("Querying cortex for context")
                    query_context = self.svc.search(prompt, COLUMNS, limit=NUM_CHUNKS)
                    print(f"Got context: {query_context}")

                    prompt = f"""
                       You are an expert chat assistance that extracts information from the CONTEXT provided
                       between <context> and </context> tags.
                       When answering the question contained between <question> and </question> tags
                       be concise and do not hallucinate. 
                       If you don´t have the information just say so.
                       Only answer the question if you can extract it from the CONTEXT provided.
            
                       Do not mention the CONTEXT used in your answer.
            
                        <context>          
                       {query_context}
                       </context>
                       <question>  
                       {rephrased_question}
                       </question>
                       Answer: 
                       """

                    json_data = json.loads(query_context.model_dump_json())
                    relative_paths = set(item['relative_path'] for item in json_data['results'])
                    print(f"Going to ask LLM the question. Relative paths: {relative_paths}")
                    
                    status.write("Generating response...")
                    cmd = """
                       select snowflake.cortex.complete(?, ?) as response
                    """

                    df_response = self.session.sql(cmd, params=['mistral-large', prompt]).collect()

                    # Update related documents
                    self.st.session_state.related_documents = []
                    if relative_paths != "None":
                        for path in relative_paths:
                            cmd2 = f"select GET_PRESIGNED_URL(@{VERIFIED_DOCUMENT_STAGE}, '{path}', 360) as URL_LINK from directory(@{VERIFIED_DOCUMENT_STAGE})"
                            df_url_link = self.session.sql(cmd2).to_pandas()
                            url_link = df_url_link._get_value(0, 'URL_LINK')
                            display_url = f"Doc: [{path}]({url_link})"
                            self.st.session_state.related_documents.append(display_url)

                    rs_text = df_response[0].RESPONSE
                    print(f"Got response: {rs_text}")
                    self.st.session_state.messages.append({"role": "assistant", "content": rs_text})
                    self.st.markdown(rs_text)
                    status.update(label="Done!", state="complete", expanded=False)

            self.st.rerun()
