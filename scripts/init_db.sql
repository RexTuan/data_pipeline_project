-- ============================================================
-- 初始化資料庫 Schema
-- 執行前請確認 PostgreSQL 已啟動：docker compose up -d postgres
-- 執行方式：在 DBeaver 開啟此檔案後全選執行
-- ============================================================

-- 移除舊表（若存在）
DROP TABLE IF EXISTS tw_stock_daily;

-- 1. 股價（每日更新）
CREATE TABLE IF NOT EXISTS tw_stock_price (
    stock_id         VARCHAR(10) NOT NULL,
    trade_date       DATE        NOT NULL,
    trading_volume   BIGINT,
    trading_money    BIGINT,
    open_price       NUMERIC,
    high_price       NUMERIC,
    low_price        NUMERIC,
    close_price      NUMERIC,
    spread           NUMERIC,
    trading_turnover REAL,
    PRIMARY KEY (stock_id, trade_date)
);

-- 2. 台股總覽（靜態，偶爾更新）
CREATE TABLE IF NOT EXISTS tw_stock_info (
    stock_id          VARCHAR(10) NOT NULL,
    stock_name        VARCHAR(50),
    industry_category VARCHAR(50),
    market_type       VARCHAR(10),
    updated_date      DATE,
    PRIMARY KEY (stock_id)
);

-- 3. 月營收（每月更新）
CREATE TABLE IF NOT EXISTS tw_stock_monthly_revenue (
    stock_id      VARCHAR(10) NOT NULL,
    date          DATE        NOT NULL,
    revenue_year  INT,
    revenue_month INT,
    country       VARCHAR(10),
    revenue       BIGINT,
    PRIMARY KEY (stock_id, date)
);

-- 4. 綜合損益表（每季更新，長格式）
CREATE TABLE IF NOT EXISTS tw_financial_statements (
    stock_id    VARCHAR(10)  NOT NULL,
    date        DATE         NOT NULL,
    type        VARCHAR(100) NOT NULL,
    value       FLOAT8,
    origin_name VARCHAR(200),
    PRIMARY KEY (stock_id, date, type)
);

-- 5. 資產負債表（每季更新，長格式）
CREATE TABLE IF NOT EXISTS tw_balance_sheet (
    stock_id    VARCHAR(10)  NOT NULL,
    date        DATE         NOT NULL,
    type        VARCHAR(100) NOT NULL,
    value       FLOAT8,
    origin_name VARCHAR(200),
    PRIMARY KEY (stock_id, date, type)
);

-- 6. 現金流量表（每季更新，長格式）
CREATE TABLE IF NOT EXISTS tw_cash_flows (
    stock_id    VARCHAR(10)  NOT NULL,
    date        DATE         NOT NULL,
    type        VARCHAR(100) NOT NULL,
    value       FLOAT8,
    origin_name VARCHAR(200),
    PRIMARY KEY (stock_id, date, type)
);
