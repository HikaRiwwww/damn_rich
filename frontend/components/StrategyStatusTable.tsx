'use client';

import { useEffect, useState } from 'react';

interface Strategy {
  id: number;
  name: string;
  name_cn: string;
  description: string;
  version: string;
  author: string;
  is_active: boolean;
  is_enabled: boolean;
  is_loaded: boolean;
  file_path: string;
  class_name: string;
  config: Record<string, any> | null;
  created_at: string | null;
  updated_at: string | null;
}

interface StrategySummary {
  total: number;
  enabled: number;
  active: number;
  loaded: number;
  disabled: number;
  inactive: number;
}

export default function StrategyStatusTable() {
  const [strategies, setStrategies] = useState<Strategy[]>([]);
  const [summary, setSummary] = useState<StrategySummary | null>(null);
  const [loading, setLoading] = useState(false);
  const [filterEnabled, setFilterEnabled] = useState<boolean | null>(null);
  const [filterActive, setFilterActive] = useState<boolean | null>(null);

  // 获取策略列表
  useEffect(() => {
    const fetchStrategies = async () => {
      setLoading(true);
      try {
        const params = new URLSearchParams();
        if (filterEnabled !== null) {
          params.append('is_enabled', String(filterEnabled));
        }
        if (filterActive !== null) {
          params.append('is_active', String(filterActive));
        }

        const response = await fetch(
          `/api/strategy/list?${params.toString()}`
        );
        const result = await response.json();
        if (result.success) {
          setStrategies(result.data);
        }
      } catch (error) {
        console.error('获取策略列表失败:', error);
      } finally {
        setLoading(false);
      }
    };

    fetchStrategies();
  }, [filterEnabled, filterActive]);

  // 获取策略状态摘要
  useEffect(() => {
    const fetchSummary = async () => {
      try {
        const response = await fetch('/api/strategy/status/summary');
        const result = await response.json();
        if (result.success) {
          setSummary(result.data);
        }
      } catch (error) {
        console.error('获取策略状态摘要失败:', error);
      }
    };

    fetchSummary();
    // 每30秒刷新一次
    const interval = setInterval(fetchSummary, 30000);
    return () => clearInterval(interval);
  }, []);

  // 刷新数据
  const refreshData = () => {
    const fetchData = async () => {
      setLoading(true);
      try {
        const params = new URLSearchParams();
        if (filterEnabled !== null) {
          params.append('is_enabled', String(filterEnabled));
        }
        if (filterActive !== null) {
          params.append('is_active', String(filterActive));
        }

        const [strategiesResponse, summaryResponse] = await Promise.all([
          fetch(`/api/strategy/list?${params.toString()}`),
          fetch('/api/strategy/status/summary'),
        ]);

        const strategiesResult = await strategiesResponse.json();
        const summaryResult = await summaryResponse.json();

        if (strategiesResult.success) {
          setStrategies(strategiesResult.data);
        }
        if (summaryResult.success) {
          setSummary(summaryResult.data);
        }
      } catch (error) {
        console.error('刷新数据失败:', error);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  };

  const getStatusBadge = (isEnabled: boolean, isActive: boolean, isLoaded: boolean) => {
    if (!isEnabled) {
      return <span style={{ padding: '2px 8px', background: '#ccc', color: '#fff', borderRadius: '4px', fontSize: '12px' }}>已禁用</span>;
    }
    if (isLoaded) {
      return <span style={{ padding: '2px 8px', background: '#52c41a', color: '#fff', borderRadius: '4px', fontSize: '12px' }}>运行中</span>;
    }
    if (isActive) {
      return <span style={{ padding: '2px 8px', background: '#1890ff', color: '#fff', borderRadius: '4px', fontSize: '12px' }}>已激活</span>;
    }
    return <span style={{ padding: '2px 8px', background: '#faad14', color: '#fff', borderRadius: '4px', fontSize: '12px' }}>待激活</span>;
  };

  return (
    <div>
      {/* 状态摘要卡片 */}
      {summary && (
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))',
            gap: '15px',
            marginBottom: '20px',
          }}
        >
          <div
            style={{
              padding: '15px',
              background: '#f5f5f5',
              borderRadius: '8px',
              border: '1px solid #ddd',
            }}
          >
            <div style={{ fontSize: '24px', fontWeight: 'bold', color: '#1890ff' }}>
              {summary.total}
            </div>
            <div style={{ fontSize: '14px', color: '#666', marginTop: '5px' }}>
              总策略数
            </div>
          </div>
          <div
            style={{
              padding: '15px',
              background: '#f5f5f5',
              borderRadius: '8px',
              border: '1px solid #ddd',
            }}
          >
            <div style={{ fontSize: '24px', fontWeight: 'bold', color: '#52c41a' }}>
              {summary.enabled}
            </div>
            <div style={{ fontSize: '14px', color: '#666', marginTop: '5px' }}>
              已启用
            </div>
          </div>
          <div
            style={{
              padding: '15px',
              background: '#f5f5f5',
              borderRadius: '8px',
              border: '1px solid #ddd',
            }}
          >
            <div style={{ fontSize: '24px', fontWeight: 'bold', color: '#1890ff' }}>
              {summary.loaded}
            </div>
            <div style={{ fontSize: '14px', color: '#666', marginTop: '5px' }}>
              运行中
            </div>
          </div>
          <div
            style={{
              padding: '15px',
              background: '#f5f5f5',
              borderRadius: '8px',
              border: '1px solid #ddd',
            }}
          >
            <div style={{ fontSize: '24px', fontWeight: 'bold', color: '#faad14' }}>
              {summary.active}
            </div>
            <div style={{ fontSize: '14px', color: '#666', marginTop: '5px' }}>
              已激活
            </div>
          </div>
          <div
            style={{
              padding: '15px',
              background: '#f5f5f5',
              borderRadius: '8px',
              border: '1px solid #ddd',
            }}
          >
            <div style={{ fontSize: '24px', fontWeight: 'bold', color: '#ff4d4f' }}>
              {summary.disabled}
            </div>
            <div style={{ fontSize: '14px', color: '#666', marginTop: '5px' }}>
              已禁用
            </div>
          </div>
        </div>
      )}

      {/* 过滤器和刷新按钮 */}
      <div
        style={{
          marginBottom: '20px',
          display: 'flex',
          gap: '15px',
          alignItems: 'center',
          flexWrap: 'wrap',
        }}
      >
        <div>
          <label style={{ marginRight: '10px' }}>启用状态:</label>
          <select
            value={filterEnabled === null ? 'all' : String(filterEnabled)}
            onChange={(e) =>
              setFilterEnabled(
                e.target.value === 'all' ? null : e.target.value === 'true'
              )
            }
            style={{ padding: '5px 10px' }}
          >
            <option value="all">全部</option>
            <option value="true">已启用</option>
            <option value="false">已禁用</option>
          </select>
        </div>
        <div>
          <label style={{ marginRight: '10px' }}>激活状态:</label>
          <select
            value={filterActive === null ? 'all' : String(filterActive)}
            onChange={(e) =>
              setFilterActive(
                e.target.value === 'all' ? null : e.target.value === 'true'
              )
            }
            style={{ padding: '5px 10px' }}
          >
            <option value="all">全部</option>
            <option value="true">已激活</option>
            <option value="false">未激活</option>
          </select>
        </div>
        <button
          onClick={refreshData}
          disabled={loading}
          style={{
            padding: '5px 15px',
            background: '#1890ff',
            color: '#fff',
            border: 'none',
            borderRadius: '4px',
            cursor: loading ? 'not-allowed' : 'pointer',
          }}
        >
          {loading ? '刷新中...' : '刷新'}
        </button>
      </div>

      {/* 策略列表表格 */}
      {loading && strategies.length === 0 ? (
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
                <th style={{ padding: '10px', border: '1px solid #ddd', textAlign: 'left' }}>
                  策略名称
                </th>
                <th style={{ padding: '10px', border: '1px solid #ddd', textAlign: 'left' }}>
                  中文名称
                </th>
                <th style={{ padding: '10px', border: '1px solid #ddd', textAlign: 'left' }}>
                  状态
                </th>
                <th style={{ padding: '10px', border: '1px solid #ddd', textAlign: 'left' }}>
                  版本
                </th>
                <th style={{ padding: '10px', border: '1px solid #ddd', textAlign: 'left' }}>
                  作者
                </th>
                <th style={{ padding: '10px', border: '1px solid #ddd', textAlign: 'left' }}>
                  描述
                </th>
                <th style={{ padding: '10px', border: '1px solid #ddd', textAlign: 'left' }}>
                  更新时间
                </th>
              </tr>
            </thead>
            <tbody>
              {strategies.length === 0 ? (
                <tr>
                  <td
                    colSpan={7}
                    style={{
                      padding: '20px',
                      textAlign: 'center',
                      border: '1px solid #ddd',
                    }}
                  >
                    暂无策略数据
                  </td>
                </tr>
              ) : (
                strategies.map((strategy) => (
                  <tr key={strategy.id}>
                    <td style={{ padding: '10px', border: '1px solid #ddd' }}>
                      <strong>{strategy.name}</strong>
                    </td>
                    <td style={{ padding: '10px', border: '1px solid #ddd' }}>
                      {strategy.name_cn || '-'}
                    </td>
                    <td style={{ padding: '10px', border: '1px solid #ddd' }}>
                      {getStatusBadge(
                        strategy.is_enabled,
                        strategy.is_active,
                        strategy.is_loaded
                      )}
                    </td>
                    <td style={{ padding: '10px', border: '1px solid #ddd' }}>
                      {strategy.version || '-'}
                    </td>
                    <td style={{ padding: '10px', border: '1px solid #ddd' }}>
                      {strategy.author || '-'}
                    </td>
                    <td
                      style={{
                        padding: '10px',
                        border: '1px solid #ddd',
                        maxWidth: '300px',
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap',
                      }}
                      title={strategy.description || ''}
                    >
                      {strategy.description || '-'}
                    </td>
                    <td style={{ padding: '10px', border: '1px solid #ddd' }}>
                      {strategy.updated_at
                        ? new Date(strategy.updated_at).toLocaleString('zh-CN')
                        : '-'}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

