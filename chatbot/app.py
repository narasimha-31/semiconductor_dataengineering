import streamlit as st
from supply_chain_bot import ask

st.set_page_config(page_title='Semiconductor Supply Chain Intel',
                   page_icon='🔌', layout='centered')

st.title('🔌 Semiconductor Supply Chain Intelligence')
st.caption('Ask questions about US chip import concentration, export-control '
           'impacts, and company financials. Powered by Grok + BigQuery over '
           'a Kafka/Airflow/dbt pipeline.')

if 'history' not in st.session_state:
    st.session_state.history = []

for entry in st.session_state.history:
    with st.chat_message('user'):
        st.write(entry['question'])
    with st.chat_message('assistant'):
        st.write(entry['answer'])
        if entry.get('sql'):
            with st.expander(f"SQL ({entry['row_count']} rows)"):
                st.code(entry['sql'], language='sql')

question = st.chat_input('e.g. Which chip category is most concentrated today?')

if question:
    with st.chat_message('user'):
        st.write(question)
    with st.chat_message('assistant'):
        with st.spinner('Generating SQL and querying BigQuery...'):
            try:
                r = ask(question)
                st.write(r['answer'])
                if r['sql']:
                    with st.expander(f"SQL ({r['row_count']} rows)"):
                        st.code(r['sql'], language='sql')
                st.session_state.history.append({
                    'question': question, 'answer': r['answer'],
                    'sql': r['sql'], 'row_count': r['row_count']})
            except Exception as e:
                st.error(f'Error: {e}')
