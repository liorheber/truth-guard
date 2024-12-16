import json

NUM_CHUNKS = 3  # Num-chunks provided as context. Play with this to check how it affects your accuracy

# columns to query in the service
COLUMNS = [
    "chunk",
    "relative_path",
]

def chat(st, session, svc):
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
        query_context = svc.search(prompt, COLUMNS, limit=NUM_CHUNKS)

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
           {prompt}
           </question>
           Answer: 
           """

        json_data = json.loads(query_context.model_dump_json())
        relative_paths = set(item['relative_path'] for item in json_data['results'])
        cmd = """
           select snowflake.cortex.complete(?, ?) as response
        """

        df_response = session.sql(cmd, params=['mistral-large', prompt]).collect()

        if relative_paths != "None":
            st.sidebar.title("Related Documents")
            for path in relative_paths:
                cmd2 = f"select GET_PRESIGNED_URL(@DOCUMENT_STAGE, '{path}', 360) as URL_LINK from directory(@DOCUMENT_STAGE)"
                df_url_link = session.sql(cmd2).to_pandas()
                url_link = df_url_link._get_value(0, 'URL_LINK')

                display_url = f"Doc: [{path}]({url_link})"
                st.sidebar.markdown(display_url)

        rs_text = df_response[0].RESPONSE
        st.session_state.messages.append({"role": "assistant", "content": rs_text})
        st.markdown(rs_text)
        st.rerun()