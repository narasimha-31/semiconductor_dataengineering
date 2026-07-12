import os
import re
from dotenv import load_dotenv
from openai import OpenAI
from google.cloud import bigquery
from pathlib import Path

load_dotenv()


_cred = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
if _cred and not os.path.isabs(_cred):
    _root = Path(__file__).resolve().parent.parent
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(_root / _cred)

PROJECT_ID = os.getenv('GCP_PROJECT_ID')
DATASET = 'semi_gold'
MODEL = 'grok-4-fast-non-reasoning'

ALLOWED_TABLES = {
    f'{PROJECT_ID}.{DATASET}.mart_hhi_concentration',
    f'{PROJECT_ID}.{DATASET}.mart_regulatory_impact',
    f'{PROJECT_ID}.{DATASET}.mart_company_signals',
}

FORBIDDEN = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER',
             'TRUNCATE', 'MERGE', 'GRANT', 'EXEC']

SCHEMA_CONTEXT = f"""
You write BigQuery SQL against these three tables. Always use fully
qualified names in backticks.

`{PROJECT_ID}.{DATASET}.mart_hhi_concentration`
  hs_code STRING - 854231=processors/GPUs, 854232=memory chips,
    854233=amplifiers, 854239=other ICs, 854290=IC parts
  trade_month DATE - first of month, 2010-01-01 to present
  hhi FLOAT - Herfindahl-Hirschman index of US import concentration
    (>2500 highly concentrated, 1500-2500 moderate, <1500 competitive)
  supplier_countries INT - countries shipping that month
  top_supplier_share FLOAT - largest single country share (0-1)
  top_supplier_country STRING - name of the largest supplier that month
  concentration_band STRING - 'highly concentrated', 'moderately
    concentrated', 'competitive'

`{PROJECT_ID}.{DATASET}.mart_regulatory_impact`
  document_number STRING, title STRING - BIS export-control rule
  publication_date DATE
  hs_code STRING - same codes as above
  avg_3mo_before FLOAT, avg_3mo_after FLOAT - avg monthly US import
    value (USD) in the 3 months before/after the rule
  pct_change_3mo FLOAT - percent change after vs before

`{PROJECT_ID}.{DATASET}.mart_company_signals`
  ticker STRING - INTC, NVDA, AMD, MU, TXN, QCOM, AVGO, GFS, TSM, ON
  period_end DATE - fiscal period end
  revenue_usd FLOAT - QUARTERLY revenue in USD, NULL for annual-only
    filers (TSM, GFS report yearly)
  revenue_fy_usd FLOAT - full fiscal-year revenue where reported
  capex_usd FLOAT - quarterly capital expenditure
  inventory_usd FLOAT - point-in-time inventory balance
  revenue_yoy_pct FLOAT - year-over-year QUARTERLY revenue change percent
  inventory_yoy_pct FLOAT - year-over-year inventory change percent;
    inventory growing much faster than revenue can signal demand
    softening or stockpiling
"""

FEW_SHOT = """
Q: Which country concentration band is US processor imports in right now?
SQL: SELECT trade_month, hhi, concentration_band FROM `{p}.{d}.mart_hhi_concentration` WHERE hs_code = '854231' ORDER BY trade_month DESC LIMIT 1

Q: What did the October 2022 export controls do to memory chip imports?
SQL: SELECT title, publication_date, avg_3mo_before, avg_3mo_after, pct_change_3mo FROM `{p}.{d}.mart_regulatory_impact` WHERE hs_code = '854232' AND publication_date BETWEEN '2022-10-01' AND '2022-10-31' ORDER BY publication_date LIMIT 10

Q: How is Nvidia's revenue growing?
SQL: SELECT period_end, revenue_usd, revenue_yoy_pct FROM `{p}.{d}.mart_company_signals` WHERE ticker = 'NVDA' AND revenue_usd IS NOT NULL ORDER BY period_end DESC LIMIT 8

Q: Which company is at the top of the semiconductor business?
SQL: SELECT ticker, period_end, revenue_usd FROM `{p}.{d}.mart_company_signals` WHERE revenue_usd IS NOT NULL AND period_end >= '2025-01-01' ORDER BY revenue_usd DESC LIMIT 10
""".format(p=PROJECT_ID, d=DATASET)

SYSTEM_PROMPT = f"""You are a SQL generator for a semiconductor supply chain
analytics platform. Given a question, respond with ONLY a BigQuery SQL
SELECT statement - no explanation, no markdown fences, no commentary.
Rules: SELECT statements only. Only the three tables described. Always
include LIMIT 100 or less.

If a question is ambiguous but reasonably answerable from these tables
(e.g. "top company" = highest recent revenue among tracked tickers),
generate SQL for the most reasonable interpretation rather than refusing.
Respond with exactly CANNOT_ANSWER only for questions truly outside these
tables: news, predictions, opinions, companies not tracked, or
non-semiconductor topics.

revenue_usd FLOAT - QUARTERLY revenue in USD; Q4 values are derived
    as fiscal year minus the three reported quarters. NULL for
    annual-only filers (TSM, GFS report yearly)
  revenue_is_derived BOOL - true when the quarterly value was derived
    from the annual figure rather than directly reported

{SCHEMA_CONTEXT}

Examples:
{FEW_SHOT}
"""

_client = None
_bq = None


def get_llm():
    global _client
    if _client is None:
        _client = OpenAI(api_key=os.getenv('XAI_API_KEY'),
                         base_url='https://api.x.ai/v1')
    return _client


def get_bq():
    global _bq
    if _bq is None:
        _bq = bigquery.Client(project=PROJECT_ID)
    return _bq


def clean_sql(raw):
    text = raw.strip()
    text = re.sub(r'^```(?:sql)?\s*', '', text)
    text = re.sub(r'\s*```$', '', text)
    return text.strip().rstrip(';')


def validate_sql(sql):
    upper = sql.upper()
    if not upper.startswith('SELECT'):
        return False, 'only SELECT statements are allowed'
    for word in FORBIDDEN:
        if re.search(rf'\b{word}\b', upper):
            return False, f'forbidden keyword: {word}'
    tables = re.findall(r'(?:FROM|JOIN)\s+`([^`]+)`', sql, re.IGNORECASE)
    if not tables:
        return False, 'no recognizable table reference'
    for t in tables:
        if t not in ALLOWED_TABLES:
            return False, f'table not allowlisted: {t}'
    if 'LIMIT' not in upper:
        sql = sql + ' LIMIT 100'
    return True, sql


def generate_sql(question):
    response = get_llm().chat.completions.create(
        model=MODEL,
        temperature=0,
        messages=[
            {'role': 'system', 'content': SYSTEM_PROMPT},
            {'role': 'user', 'content': question}
        ]
    )
    return clean_sql(response.choices[0].message.content)


def run_query(sql):
    job = get_bq().query(sql)
    return [dict(row) for row in job.result()]


def summarize(question, rows):
    response = get_llm().chat.completions.create(
        model=MODEL,
        temperature=0,
        messages=[
            {'role': 'system', 'content':
                'You are a supply chain analyst. Answer the question in 2-4 '
                'sentences using ONLY the data provided. Cite numbers. State '
                'the scope of the data you are reporting: trade figures are '
                'US imports in USD by HS product category; company figures '
                'are quarterly or fiscal-year values from SEC filings. If '
                'the rows are empty, say no data was found. Never speculate '
                'beyond the rows.'},
            {'role': 'user', 'content':
                f'Question: {question}\n\nQuery results:\n{rows[:50]}'}
        ]
    )
    return response.choices[0].message.content


def ask(question):
    raw = generate_sql(question)
    result = {'raw': raw, 'sql': None, 'answer': None,
              'blocked': False, 'row_count': 0}

    if raw == 'CANNOT_ANSWER':
        result['answer'] = ("This can't be answered from the supply chain "
                            "marts (trade concentration, regulatory impact, "
                            "company financials).")
        return result

    ok, validated = validate_sql(raw)
    if not ok:
        result['blocked'] = True
        result['answer'] = f'Blocked by guardrail: {validated}'
        return result

    result['sql'] = validated
    rows = run_query(validated)
    result['row_count'] = len(rows)
    result['answer'] = summarize(question, rows)
    return result


if __name__ == '__main__':
    print('Semiconductor Supply Chain Intelligence - ask me anything.')
    print("(type 'quit' to exit)\n")
    while True:
        q = input('You: ').strip()
        if q.lower() in ('quit', 'exit'):
            break
        try:
            r = ask(q)
            print(f"\n[RAW] {r['raw']}")
            if r['sql']:
                print(f"[SQL] {r['sql']}  ({r['row_count']} rows)")
            print(f"Bot: {r['answer']}\n")
        except Exception as e:
            print(f'Error: {e}\n')
