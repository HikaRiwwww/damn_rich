'use client';

import KlineDataTable from '@/components/KlineDataTable';
import StrategyStatusTable from '@/components/StrategyStatusTable';
import TradingRecordsTable from '@/components/TradingRecordsTable';
import { useState } from 'react';

export default function Home() {
  const [activeTab, setActiveTab] = useState<'kline' | 'trades' | 'strategy'>('kline');

  return (
    <div style={{ padding: '20px' }}>
      <h1 style={{ marginBottom: '20px' }}>Damn Rich - 量化交易监控</h1>
      
      {/* 标签页 */}
      <div style={{ marginBottom: '20px', borderBottom: '1px solid #ccc' }}>
        <button
          onClick={() => setActiveTab('kline')}
          style={{
            padding: '10px 20px',
            marginRight: '10px',
            border: 'none',
            borderBottom: activeTab === 'kline' ? '2px solid #0070f3' : 'none',
            background: 'none',
            cursor: 'pointer',
          }}
        >
          K线数据
        </button>
        <button
          onClick={() => setActiveTab('trades')}
          style={{
            padding: '10px 20px',
            marginRight: '10px',
            border: 'none',
            borderBottom: activeTab === 'trades' ? '2px solid #0070f3' : 'none',
            background: 'none',
            cursor: 'pointer',
          }}
        >
          交易记录
        </button>
        <button
          onClick={() => setActiveTab('strategy')}
          style={{
            padding: '10px 20px',
            marginRight: '10px',
            border: 'none',
            borderBottom: activeTab === 'strategy' ? '2px solid #0070f3' : 'none',
            background: 'none',
            cursor: 'pointer',
          }}
        >
          策略状态
        </button>
      </div>

      {/* 内容区域 */}
      {activeTab === 'kline' && <KlineDataTable />}
      {activeTab === 'trades' && <TradingRecordsTable />}
      {activeTab === 'strategy' && <StrategyStatusTable />}
    </div>
  );
}

