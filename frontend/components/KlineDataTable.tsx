'use client';

import dayjs from 'dayjs';
import timezone from 'dayjs/plugin/timezone';
import utc from 'dayjs/plugin/utc';
import { useEffect, useState } from 'react';

dayjs.extend(utc);
dayjs.extend(timezone);

interface KlineData {
  id: number;
  timestamp: number;
  datetime: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  symbol_id: number;
  exchange_id: number;
  timeframe: string;
}

interface Symbol {
  id: number;
  symbol: string;
  base_currency: string;
  quote_currency: string;
  is_active: boolean;
  is_trading: boolean;
  base_precision?: number;
  quote_precision?: number;
  min_order_size?: number;
  max_order_size?: number;
}

interface TimezoneOption {
  value: string;
  label: string;
  tz: string; // IANA 时区标识符
}

const TIMEZONES: TimezoneOption[] = [
  { value: 'UTC+8', label: '北京时间 (UTC+8)', tz: 'Asia/Shanghai' },
  { value: 'UTC+0', label: 'UTC (UTC+0)', tz: 'UTC' },
  { value: 'UTC-5', label: '美东时间 (UTC-5)', tz: 'America/New_York' },
  { value: 'UTC+1', label: '中欧时间 (UTC+1)', tz: 'Europe/Berlin' },
  { value: 'UTC+9', label: '日本时间 (UTC+9)', tz: 'Asia/Tokyo' },
];

export default function KlineDataTable() {
  const [symbols, setSymbols] = useState<Symbol[]>([]);
  const [selectedSymbol, setSelectedSymbol] = useState<number | null>(null);
  const [klineData, setKlineData] = useState<KlineData[]>([]);
  const [loading, setLoading] = useState(false);
  const [selectedTimezone, setSelectedTimezone] = useState<string>('UTC+8');

  // 获取交易对列表
  useEffect(() => {
    const fetchSymbols = async () => {
      try {
        const response = await fetch('/api/data-sync/symbols');
        const result = await response.json();
        if (result.success) {
          setSymbols(result.data);
          // 默认选择第一个交易对
          if (result.data.length > 0) {
            setSelectedSymbol(result.data[0].id);
          }
        }
      } catch (error) {
        console.error('获取交易对列表失败:', error);
      }
    };

    fetchSymbols();
  }, []);

  // 获取 K 线数据
  useEffect(() => {
    if (selectedSymbol) {
      const fetchKlineData = async () => {
        setLoading(true);
        try {
          const response = await fetch(
            `/api/data-sync/kline-data?symbol_id=${selectedSymbol}&limit=100`
          );
          const result = await response.json();
          if (result.success) {
            setKlineData(result.data);
          }
        } catch (error) {
          console.error('获取K线数据失败:', error);
        } finally {
          setLoading(false);
        }
      };

      fetchKlineData();
    }
  }, [selectedSymbol]);

  // 从时间戳格式化时间函数
  const formatDateTimeFromTimestamp = (timestamp: number, timezone: string): string => {
    try {
      const timezoneOption = TIMEZONES.find(tz => tz.value === timezone);
      if (!timezoneOption) {
        return dayjs(timestamp).format('YYYY-MM-DD HH:mm:ss');
      }

      // 使用 dayjs 将 UTC 时间戳转换为目标时区
      return dayjs(timestamp).utc().tz(timezoneOption.tz).format('YYYY-MM-DD HH:mm:ss');
    } catch (error) {
      return dayjs(timestamp).format('YYYY-MM-DD HH:mm:ss');
    }
  };

  return (
    <div>
      {/* 交易对选择和时区选择 */}
      <div style={{ marginBottom: '20px', display: 'flex', gap: '20px', alignItems: 'center' }}>
        <div>
          <label style={{ marginRight: '10px' }}>选择交易对:</label>
          <select
            value={selectedSymbol || ''}
            onChange={(e) => setSelectedSymbol(Number(e.target.value))}
            style={{ padding: '5px 10px' }}
          >
            <option value="">请选择</option>
            {symbols.map((symbol) => (
              <option key={symbol.id} value={symbol.id}>
                {symbol.symbol}
              </option>
            ))}
          </select>
        </div>
        <div>
          <label style={{ marginRight: '10px' }}>时区:</label>
          <select
            value={selectedTimezone}
            onChange={(e) => setSelectedTimezone(e.target.value)}
            style={{ padding: '5px 10px' }}
          >
            {TIMEZONES.map((tz) => (
              <option key={tz.value} value={tz.value}>
                {tz.label}
              </option>
            ))}
          </select>
        </div>
      </div>

      {/* K 线数据表格 */}
      {loading ? (
        <div>加载中...</div>
      ) : (
        <div style={{ overflowX: 'auto' }}>
          <table
            style={{
              width: '100%',
              borderCollapse: 'collapse',
              fontSize: '14px',
            }}
          >
            <thead>
              <tr style={{ backgroundColor: '#f5f5f5' }}>
                <th style={{ padding: '10px', border: '1px solid #ddd' }}>时间</th>
                <th style={{ padding: '10px', border: '1px solid #ddd' }}>开盘价</th>
                <th style={{ padding: '10px', border: '1px solid #ddd' }}>最高价</th>
                <th style={{ padding: '10px', border: '1px solid #ddd' }}>最低价</th>
                <th style={{ padding: '10px', border: '1px solid #ddd' }}>收盘价</th>
                <th style={{ padding: '10px', border: '1px solid #ddd' }}>成交量</th>
              </tr>
            </thead>
            <tbody>
              {klineData.map((kline) => (
                <tr key={kline.id}>
                  <td style={{ padding: '10px', border: '1px solid #ddd' }}>
                    {formatDateTimeFromTimestamp(kline.timestamp, selectedTimezone)}
                  </td>
                  <td style={{ padding: '10px', border: '1px solid #ddd', textAlign: 'right' }}>
                    {kline.open?.toFixed(2)}
                  </td>
                  <td style={{ padding: '10px', border: '1px solid #ddd', textAlign: 'right' }}>
                    {kline.high?.toFixed(2)}
                  </td>
                  <td style={{ padding: '10px', border: '1px solid #ddd', textAlign: 'right' }}>
                    {kline.low?.toFixed(2)}
                  </td>
                  <td style={{ padding: '10px', border: '1px solid #ddd', textAlign: 'right' }}>
                    {kline.close?.toFixed(2)}
                  </td>
                  <td style={{ padding: '10px', border: '1px solid #ddd', textAlign: 'right' }}>
                    {kline.volume?.toFixed(4)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

