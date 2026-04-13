import psycopg2
import requests
import os
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("FINMIND_TOKEN")

BASE_URL = "https://api.finmindtrade.com/api/v4/data"


# ── 通用 API 抓取 ──────────────────────────────────────────────────────────────
def fetch_data(dataset, token, stock_id=None, start_date=None, end_date=None):
    """通用 FinMind API 抓取函數，適用所有 Dataset"""
    params = {"dataset": dataset, "token": token}
    if stock_id:
        params["data_id"] = stock_id
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()

    result = response.json()
    print(f"[{dataset}] API 回應: {result.get('msg')}")
    return result.get("data", [])


# ── 各表專屬 Upsert 函數 ───────────────────────────────────────────────────────
def upsert_stock_price(cur, row):
    sql = """
        INSERT INTO tw_stock_price
            (stock_id, trade_date, open_price, high_price, low_price, close_price,
             trading_volume, trading_money, spread, trading_turnover)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (stock_id, trade_date) DO UPDATE SET
            open_price       = EXCLUDED.open_price,
            high_price       = EXCLUDED.high_price,
            low_price        = EXCLUDED.low_price,
            close_price      = EXCLUDED.close_price,
            trading_volume   = EXCLUDED.trading_volume,
            trading_money    = EXCLUDED.trading_money,
            spread           = EXCLUDED.spread,
            trading_turnover = EXCLUDED.trading_turnover;
    """
    cur.execute(sql, (
        row["stock_id"], row["date"],
        row["open"], row["max"], row["min"], row["close"],
        row["Trading_Volume"], row["Trading_money"], row["spread"], row["Trading_turnover"]
    ))


def upsert_stock_info(cur, row):
    sql = """
        INSERT INTO tw_stock_info
            (stock_id, stock_name, industry_category, market_type, updated_date)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (stock_id) DO UPDATE SET
            stock_name        = EXCLUDED.stock_name,
            industry_category = EXCLUDED.industry_category,
            market_type       = EXCLUDED.market_type,
            updated_date      = EXCLUDED.updated_date;
    """
    cur.execute(sql, (
        row["stock_id"], row["stock_name"],
        row["industry_category"], row["type"], row["date"]
    ))


def upsert_monthly_revenue(cur, row):
    sql = """
        INSERT INTO tw_stock_monthly_revenue
            (stock_id, date, revenue_year, revenue_month, country, revenue)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (stock_id, date) DO UPDATE SET
            revenue       = EXCLUDED.revenue,
            country       = EXCLUDED.country;
    """
    cur.execute(sql, (
        row["stock_id"], row["date"],
        row["revenue_year"], row["revenue_month"], row["country"], row["revenue"]
    ))


def make_financial_upsert(table_name):
    """
    工廠函數：產生長格式財報表的 Upsert 函數
    適用：tw_financial_statements, tw_balance_sheet, tw_cash_flows（三表結構相同）
    """
    def upsert(cur, row):
        sql = f"""
            INSERT INTO {table_name}
                (stock_id, date, type, value, origin_name)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (stock_id, date, type) DO UPDATE SET
                value       = EXCLUDED.value,
                origin_name = EXCLUDED.origin_name;
        """
        cur.execute(sql, (
            row["stock_id"], row["date"],
            row["type"], row["value"], row["origin_name"]
        ))
    return upsert


# ── 資料集設定對照表 ───────────────────────────────────────────────────────────
# 新增表格時，只需在這裡加一個設定即可
DATASET_CONFIG = {
    "TaiwanStockPrice": {
        "table":    "tw_stock_price",
        "upsert_fn": upsert_stock_price,
    },
    "TaiwanStockInfo": {
        "table":    "tw_stock_info",
        "upsert_fn": upsert_stock_info,
    },
    "TaiwanStockMonthRevenue": {
        "table":    "tw_stock_monthly_revenue",
        "upsert_fn": upsert_monthly_revenue,
    },
    "TaiwanStockFinancialStatements": {
        "table":    "tw_financial_statements",
        "upsert_fn": make_financial_upsert("tw_financial_statements"),
    },
    "TaiwanStockBalanceSheet": {
        "table":    "tw_balance_sheet",
        "upsert_fn": make_financial_upsert("tw_balance_sheet"),
    },
    "TaiwanStockCashFlowsStatement": {
        "table":    "tw_cash_flows",
        "upsert_fn": make_financial_upsert("tw_cash_flows"),
    },
}


# ── 通用寫入 PostgreSQL ────────────────────────────────────────────────────────
def upsert_to_postgres(dataset, data_list, host="localhost"):
    """依 dataset 名稱自動選擇對應的 Upsert 邏輯並寫入"""
    config = DATASET_CONFIG[dataset]

    conn = psycopg2.connect(host=host, database="airflow", user="airflow", password="airflow")
    cur = conn.cursor()

    for row in data_list:
        config["upsert_fn"](cur, row)

    conn.commit()
    cur.close()
    conn.close()
    print(f"[{dataset}] 成功寫入 {len(data_list)} 筆資料至 {config['table']}")


# ── 主函數（供 Airflow DAG 呼叫）─────────────────────────────────────────────
def fetch_and_upsert(dataset, token, stock_id=None, start_date=None, end_date=None, db_host="postgres"):
    """Airflow DAG 呼叫的通用主函數"""
    data_list = fetch_data(dataset, token, stock_id, start_date, end_date)

    if not data_list:
        print(f"[{dataset}] 查無資料")
        return

    upsert_to_postgres(dataset, data_list, host=db_host)


# ── 本機測試用（直接執行此檔案時才會跑）────────────────────────────────────────
if __name__ == "__main__":

    # 觀察名單
    WATCHLIST = [
        "2330",  # 台積電
        "2308",  # 台達電
        "2317",  # 鴻海
        "2454",  # 聯發科
        "3711",  # 日月光投控
        "0050",  # 元大台灣50
        "2382",  # 廣達
        "2881",  # 富邦金
        "2603",  # 長榮
        "2357",  # 華碩
        "3231",  # 緯創
        "2207",  # 和泰車
        "2353",  # 宏碁
        "2542",  # 興富發
        "3293",  # 鈊象
    ]

    # 1. 台股總覽（全部，只需跑一次，不需依股票代號）
    fetch_and_upsert("TaiwanStockInfo", TOKEN,
                     db_host="localhost")

    # 2-6. 其餘五張表，依觀察名單逐支抓取
    for stock_id in WATCHLIST:
        print(f"\n===== 開始處理 {stock_id} =====")

        # 股價（近兩年）
        fetch_and_upsert("TaiwanStockPrice", TOKEN,
                         stock_id=stock_id, start_date="2024-01-01", end_date="2025-12-31",
                         db_host="localhost")

        # 月營收（近兩年）
        fetch_and_upsert("TaiwanStockMonthRevenue", TOKEN,
                         stock_id=stock_id, start_date="2024-01-01",
                         db_host="localhost")

        # 綜合損益表（近三年）
        fetch_and_upsert("TaiwanStockFinancialStatements", TOKEN,
                         stock_id=stock_id, start_date="2022-01-01",
                         db_host="localhost")

        # 資產負債表（近三年）
        fetch_and_upsert("TaiwanStockBalanceSheet", TOKEN,
                         stock_id=stock_id, start_date="2022-01-01",
                         db_host="localhost")

        # 現金流量表（近三年）
        fetch_and_upsert("TaiwanStockCashFlowsStatement", TOKEN,
                         stock_id=stock_id, start_date="2022-01-01",
                         db_host="localhost")
