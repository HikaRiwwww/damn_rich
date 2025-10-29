-- 加密货币量化交易系统 - 数据库初始化脚本
-- 数据库: crypto_trading

-- ==========================================
-- 1. 创建 exchanges 表 (交易所信息)
-- ==========================================
CREATE TABLE IF NOT EXISTS exchanges (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    display_name VARCHAR(100),
    country VARCHAR(50),
    website VARCHAR(200),
    api_base_url VARCHAR(200),
    websocket_url VARCHAR(200),
    rate_limit INTEGER,
    is_active BOOLEAN DEFAULT TRUE,
    is_sandbox_supported BOOLEAN DEFAULT FALSE,
    sandbox_url VARCHAR(200),
    description TEXT
);

COMMENT ON TABLE exchanges IS '交易所信息表';
COMMENT ON COLUMN exchanges.id IS '主键ID';
COMMENT ON COLUMN exchanges.name IS '交易所名称';
COMMENT ON COLUMN exchanges.display_name IS '交易所显示名称';
COMMENT ON COLUMN exchanges.country IS '所在国家';
COMMENT ON COLUMN exchanges.website IS '官方网站';
COMMENT ON COLUMN exchanges.api_base_url IS 'API基础URL';
COMMENT ON COLUMN exchanges.websocket_url IS 'WebSocket URL';
COMMENT ON COLUMN exchanges.rate_limit IS 'API限制频率(毫秒)';
COMMENT ON COLUMN exchanges.is_active IS '是否激活';
COMMENT ON COLUMN exchanges.is_sandbox_supported IS '是否支持沙盒';
COMMENT ON COLUMN exchanges.sandbox_url IS '沙盒环境URL';
COMMENT ON COLUMN exchanges.description IS '交易所描述';

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_exchange_name ON exchanges(name);
CREATE INDEX IF NOT EXISTS idx_exchange_active ON exchanges(is_active);

-- 插入币安交易所数据
INSERT INTO exchanges (
    name, display_name, country, website, 
    api_base_url, websocket_url, rate_limit,
    is_active, is_sandbox_supported, sandbox_url, description
) VALUES (
    'binance', 
    'Binance', 
    'Cayman Islands',
    'https://www.binance.com',
    'https://api.binance.com',
    'wss://stream.binance.com:9443',
    100,
    TRUE,
    TRUE,
    'https://testnet.binance.vision',
    '全球最大的加密货币交易所'
) ON CONFLICT (name) DO NOTHING;

-- ==========================================
-- 2. 创建 symbols 表 (交易对信息)
-- ==========================================
CREATE TABLE IF NOT EXISTS symbols (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    base_asset VARCHAR(20) NOT NULL,
    quote_asset VARCHAR(20) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    is_trading BOOLEAN DEFAULT TRUE,
    base_precision INTEGER,
    quote_precision INTEGER,
    min_order_size DOUBLE PRECISION,
    max_order_size DOUBLE PRECISION
);

COMMENT ON TABLE symbols IS '交易对信息表';
COMMENT ON COLUMN symbols.id IS '主键ID';
COMMENT ON COLUMN symbols.symbol IS '交易对符号';
COMMENT ON COLUMN symbols.base_asset IS '基础资产';
COMMENT ON COLUMN symbols.quote_asset IS '计价资产';
COMMENT ON COLUMN symbols.is_active IS '是否激活';
COMMENT ON COLUMN symbols.is_trading IS '是否可交易';
COMMENT ON COLUMN symbols.base_precision IS '基础资产精度';
COMMENT ON COLUMN symbols.quote_precision IS '计价资产精度';
COMMENT ON COLUMN symbols.min_order_size IS '最小订单数量';
COMMENT ON COLUMN symbols.max_order_size IS '最大订单数量';

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_symbol ON symbols(symbol);
CREATE INDEX IF NOT EXISTS idx_base_quote ON symbols(base_asset, quote_asset);
CREATE INDEX IF NOT EXISTS idx_symbol_active ON symbols(is_active);

-- 插入常见的交易对 (示例)
INSERT INTO symbols (symbol, base_asset, quote_asset, is_active, is_trading) VALUES
('BTC/USDT', 'BTC', 'USDT', TRUE, TRUE),
('ETH/USDT', 'ETH', 'USDT', TRUE, TRUE),
('BNB/USDT', 'BNB', 'USDT', TRUE, TRUE),
('SOL/USDT', 'SOL', 'USDT', TRUE, TRUE),
('ADA/USDT', 'ADA', 'USDT', TRUE, TRUE)
ON CONFLICT (symbol) DO NOTHING;

-- ==========================================
-- 3. 创建 kline_data 表 (K线数据)
-- ==========================================
CREATE TABLE IF NOT EXISTS kline_data (
    id BIGSERIAL PRIMARY KEY,
    exchange_id INTEGER NOT NULL,
    symbol_id INTEGER NOT NULL,
    timeframe VARCHAR(10) NOT NULL,
    timestamp BIGINT NOT NULL,
    datetime TIMESTAMP NOT NULL,
    open_price DOUBLE PRECISION NOT NULL,
    high_price DOUBLE PRECISION NOT NULL,
    low_price DOUBLE PRECISION NOT NULL,
    close_price DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,
    quote_volume DOUBLE PRECISION,
    trades_count INTEGER,
    taker_buy_base_volume DOUBLE PRECISION,
    taker_buy_quote_volume DOUBLE PRECISION
);

COMMENT ON TABLE kline_data IS 'K线数据表';
COMMENT ON COLUMN kline_data.id IS '主键ID';
COMMENT ON COLUMN kline_data.exchange_id IS '交易所ID';
COMMENT ON COLUMN kline_data.symbol_id IS '交易对ID';
COMMENT ON COLUMN kline_data.timeframe IS '时间周期';
COMMENT ON COLUMN kline_data.timestamp IS 'K线时间戳(毫秒)';
COMMENT ON COLUMN kline_data.datetime IS 'K线时间';
COMMENT ON COLUMN kline_data.open_price IS '开盘价';
COMMENT ON COLUMN kline_data.high_price IS '最高价';
COMMENT ON COLUMN kline_data.low_price IS '最低价';
COMMENT ON COLUMN kline_data.close_price IS '收盘价';
COMMENT ON COLUMN kline_data.volume IS '成交量';
COMMENT ON COLUMN kline_data.quote_volume IS '成交额';
COMMENT ON COLUMN kline_data.trades_count IS '成交笔数';
COMMENT ON COLUMN kline_data.taker_buy_base_volume IS '主动买入成交量';
COMMENT ON COLUMN kline_data.taker_buy_quote_volume IS '主动买入成交额';

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_exchange_symbol_timeframe ON kline_data(exchange_id, symbol_id, timeframe);
CREATE INDEX IF NOT EXISTS idx_timestamp ON kline_data(timestamp);
CREATE INDEX IF NOT EXISTS idx_datetime ON kline_data(datetime);
CREATE INDEX IF NOT EXISTS idx_exchange_symbol_timestamp ON kline_data(exchange_id, symbol_id, timestamp);

-- 为了避免重复数据，建议创建唯一约束
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_kline 
ON kline_data(exchange_id, symbol_id, timeframe, timestamp);

-- ==========================================
-- 4. 创建迁移记录表 (可选)
-- ==========================================
CREATE TABLE IF NOT EXISTS migrations (
    version VARCHAR(50) PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE migrations IS '数据库迁移记录表';

-- ==========================================
-- 完成
-- ==========================================
-- 查看已创建的表
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' 
  AND table_name IN ('exchanges', 'symbols', 'kline_data', 'migrations');

