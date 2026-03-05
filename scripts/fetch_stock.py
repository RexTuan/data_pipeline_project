import psycopg2
import requests


def fetch_stock_data(stock_id, target_date, token):
    """只負責呼叫 FinMind API，回傳原始資料列表"""
    url = "https://api.finmindtrade.com/api/v4/data"
    params = {
        "dataset": "TaiwanStockPrice",
        "data_id": stock_id,
        "start_date": target_date,
        "end_date": target_date,
        "token": token,
    }
    response = requests.get(url, params=params)
    response.raise_for_status()

    result = response.json()
    print(f"API 回應狀態: {result.get('msg')}")
    return result.get("data", [])


def upsert_to_postgres(data_list, host="localhost"):
    """將資料寫入 PostgreSQL"""
    conn = psycopg2.connect(
        host=host,
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    upsert_sql = """
        INSERT INTO tw_stock_daily
            (stock_id, trade_date, open_price, high_price, low_price, close_price, trade_volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (stock_id, trade_date)
        DO UPDATE SET
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            trade_volume = EXCLUDED.trade_volume;
    """

    for row in data_list:
        cur.execute(upsert_sql, (
            row["stock_id"],
            row["date"],
            row["open"],
            row["max"],
            row["min"],
            row["close"],
            row["Trading_Volume"],
        ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"成功寫入 {len(data_list)} 筆資料")


def fetch_and_upsert_stock(stock_id, target_date, token, db_host="postgres"):
    """Airflow DAG 呼叫的主函數"""
    data_list = fetch_stock_data(stock_id, target_date, token)

    if not data_list:
        print(f"No data found for {stock_id} on {target_date}")
        return

    upsert_to_postgres(data_list, host=db_host)


# ── 本機測試用（直接執行此檔案時才會跑） ──
if __name__ == "__main__":
    TOKEN = "貼上你的FinMind_Token"
    STOCK_ID = "2330"
    DATE = "2025-01-02"  # 選一個過去的交易日

    # 先只測試 API，不寫 DB
    data = fetch_stock_data(STOCK_ID, DATE, TOKEN)
    print(f"取得 {len(data)} 筆資料")
    if data:
        print("第一筆資料:", data[0])
