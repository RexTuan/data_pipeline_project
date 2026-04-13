import os
import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery

# ── 設定 GCP 認證金鑰路徑 ────────────────────────────────────────────────────
KEY_PATH = os.path.join(os.path.dirname(__file__), '..', 'keys', 'gen-lang-client-0943949278-40c018a659f4.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.abspath(KEY_PATH)

# ── 設定常數 ─────────────────────────────────────────────────────────────────
PROJECT_ID = 'gen-lang-client-0943949278'
DATASET_ID = 'stock_analytics'
PG_URI     = 'postgresql+psycopg2://airflow:airflow@localhost:5432/airflow'

# ── 要同步的資料表清單 ────────────────────────────────────────────────────────
TABLES = [
    'tw_stock_price',
    'tw_stock_info',
    'tw_stock_monthly_revenue',
    'tw_financial_statements',
    'tw_balance_sheet',
    'tw_cash_flows',
]


# ── 單張表同步函數 ────────────────────────────────────────────────────────────
def sync_table_to_bq(table_name, engine, client):
    """從 PostgreSQL 讀取整張表，以 WRITE_TRUNCATE 策略寫入 BigQuery"""
    print(f'[{table_name}] 開始同步...')

    # 1. 從 PostgreSQL 讀取資料
    df = pd.read_sql(f'SELECT * FROM {table_name}', engine)

    if df.empty:
        print(f'[{table_name}] 無資料，略過')
        return

    # 2. 設定目標表 ID
    table_id = f'{PROJECT_ID}.{DATASET_ID}.{table_name}'

    # 3. 設定載入組態（WRITE_TRUNCATE = 完全覆寫，解決沙盒不支援 DML 的限制）
    job_config = bigquery.LoadJobConfig(
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
    )

    # 4. 上傳至 BigQuery
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # 等待完成

    print(f'[{table_name}] 成功同步 {job.output_rows} 筆資料至 BigQuery ✅')


# ── 主函數（同步所有資料表）────────────────────────────────────────────────────
def sync_all_tables(pg_host='localhost'):
    pg_uri = f'postgresql+psycopg2://airflow:airflow@{pg_host}:5432/airflow'
    engine = create_engine(pg_uri)
    client = bigquery.Client(project=PROJECT_ID)

    for table_name in TABLES:
        sync_table_to_bq(table_name, engine, client)

    print('\n全部同步完成！🎉')


# ── 本機測試用 ────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    sync_all_tables(pg_host='localhost')
