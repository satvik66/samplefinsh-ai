import os
import pandas as pd
import streamlit as st
from engine import initialize, seed_synthetic, migrate, metrics, inspect_issue, assistant_answer

st.set_page_config(page_title='FinShift AI | Migration Intelligence', page_icon='🔁',layout='wide')
st.markdown('''<style>.block-container{padding-top:1.6rem;max-width:1300px}h1{letter-spacing:-.04em}.stMetric{border:1px solid rgba(140,150,170,.2);padding:14px;border-radius:12px}</style>''',unsafe_allow_html=True)
initialize()
st.title('FinShift AI')
st.caption('Enterprise Data Migration & AI Reliability Copilot · Independent portfolio demonstration · Synthetic financial data only')
with st.sidebar:
    st.header('Migration operations')
    st.caption('Legacy source: SQLite simulation of a financial transaction database')
    n=st.number_input('Synthetic source rows',min_value=20,max_value=10000,value=750,step=100)
    inject=st.checkbox('Inject realistic quality errors',value=True)
    if st.button('1 · Generate synthetic source data',use_container_width=True):
        seed_synthetic(count=int(n),inject_errors=inject)
        st.success('Generated reproducible synthetic legacy records.')
    mode=st.radio('Load strategy',['full','incremental'],horizontal=True)
    if st.button('2 · Run migration',type='primary',use_container_width=True):
        result=migrate(mode=mode)
        st.success(f"Run #{result['run_id']}: {result['accepted']} accepted, {result['rejected']} rejected")
    st.divider()
    st.caption('Full refresh rebuilds Bronze/Silver. Incremental loads only rows with later updated_at timestamps. All processing is local.')
    st.caption('Production targets such as Snowflake, Kafka, EMR, and Airflow are architectural extension points, not connected services in this demo.')

m=metrics()
st.subheader('Migration control center')
a,b,c,d=st.columns(4)
a.metric('Legacy rows',f"{m['legacy_transactions']:,}")
b.metric('Bronze rows',f"{m['bronze_transactions']:,}")
c.metric('Validated Silver rows',f"{m['silver_transactions']:,}")
d.metric('Reconciliation', 'PASS' if m['source_target_match'] and m['runs'] else 'NOT RUN / REVIEW')

t1,t2,t3,t4=st.tabs(['Migration Pipeline','Data Quality & Reconciliation','Analytics Warehouse','AI Migration Copilot'])
with t1:
    st.markdown('#### Source → Bronze → Validated Silver → Gold')
    st.info('Start with **Generate synthetic source data**, then **Run migration** in the sidebar.')
    if m['runs']:
        df=pd.DataFrame(m['runs']).drop(columns=['diagnostics'])
        st.dataframe(df,use_container_width=True,hide_index=True)
    else: st.caption('No migration executions yet.')
    st.markdown('**Implementation design:** idempotent source_id upsert, watermark-based incremental processing, error quarantine, atomic transaction, and materialized daily aggregates.')
with t2:
    x,y,z=st.columns(3)
    x.metric('Latest run rejected',m['runs'][0]['rejected'] if m['runs'] else 0)
    y.metric('Accepted source amount',f"${m['source_amount']:,.2f}")
    z.metric('Silver amount',f"${m['target_amount']:,.2f}")
    st.write('**Reconciliation evidence:**',inspect_issue())
    if m['rejections']:
        st.dataframe(pd.DataFrame(m['rejections']),use_container_width=True,hide_index=True)
    else: st.caption('No rejection events recorded.')
with t3:
    st.markdown('#### Gold-layer financial transaction analytics')
    if m['daily']:
        daily=pd.DataFrame(m['daily'])
        st.line_chart(daily.set_index('event_day')[['amount_usd']])
        st.dataframe(daily,use_container_width=True,hide_index=True)
    else: st.caption('The Gold table will populate after a migration run.')
with t4:
    st.markdown('#### Grounded migration investigation')
    st.caption('This assistant uses only current migration run metadata and validation evidence. Without an API key it provides a deterministic offline diagnosis.')
    key=st.text_input('OpenAI API key (optional; session only)',value=os.getenv('OPENAI_API_KEY',''),type='password')
    prompt=st.text_area('Ask about the migration',value='Why did the last migration reject records, and what should I do next?')
    if st.button('Analyze migration evidence',type='primary'):
        st.markdown(assistant_answer(prompt,api_key=key))
st.divider()
st.caption('Demonstration only. No real customer or employer data. No live financial institution, cloud warehouse, or production services are accessed.')
