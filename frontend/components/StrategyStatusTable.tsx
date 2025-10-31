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

// 配置编辑模态框组件
interface ConfigModalProps {
  strategy: Strategy | null;
  isOpen: boolean;
  onClose: () => void;
  onSave: (config: Record<string, any>) => Promise<void>;
}

function ConfigModal({ strategy, isOpen, onClose, onSave }: ConfigModalProps) {
  const [configText, setConfigText] = useState('');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (strategy && isOpen) {
      setConfigText(JSON.stringify(strategy.config || {}, null, 2));
      setError(null);
    }
  }, [strategy, isOpen]);

  if (!isOpen || !strategy) return null;

  const handleSave = async () => {
    try {
      setSaving(true);
      setError(null);
      const config = JSON.parse(configText);
      await onSave(config);
      onClose();
    } catch (e) {
      setError('配置格式错误：' + (e instanceof Error ? e.message : String(e)));
    } finally {
      setSaving(false);
    }
  };

  return (
    <div
      style={{
        position: 'fixed',
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        backgroundColor: 'rgba(0, 0, 0, 0.5)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        zIndex: 1000,
      }}
      onClick={onClose}
    >
      <div
        style={{
          backgroundColor: '#fff',
          padding: '20px',
          borderRadius: '8px',
          maxWidth: '600px',
          width: '90%',
          maxHeight: '80vh',
          overflow: 'auto',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        <h3 style={{ marginTop: 0 }}>编辑策略配置 - {strategy.name_cn || strategy.name}</h3>
        {error && (
          <div style={{ padding: '10px', background: '#ffebee', color: '#c62828', borderRadius: '4px', marginBottom: '10px' }}>
            {error}
          </div>
        )}
        <textarea
          value={configText}
          onChange={(e) => setConfigText(e.target.value)}
          style={{
            width: '100%',
            height: '400px',
            fontFamily: 'monospace',
            fontSize: '12px',
            padding: '10px',
            border: '1px solid #ddd',
            borderRadius: '4px',
          }}
          placeholder="输入JSON配置..."
        />
        <div style={{ marginTop: '15px', display: 'flex', gap: '10px', justifyContent: 'flex-end' }}>
          <button
            onClick={onClose}
            disabled={saving}
            style={{
              padding: '8px 16px',
              background: '#f5f5f5',
              border: '1px solid #ddd',
              borderRadius: '4px',
              cursor: saving ? 'not-allowed' : 'pointer',
            }}
          >
            取消
          </button>
          <button
            onClick={handleSave}
            disabled={saving}
            style={{
              padding: '8px 16px',
              background: '#1890ff',
              color: '#fff',
              border: 'none',
              borderRadius: '4px',
              cursor: saving ? 'not-allowed' : 'pointer',
            }}
          >
            {saving ? '保存中...' : '保存'}
          </button>
        </div>
      </div>
    </div>
  );
}

export default function StrategyStatusTable() {
  const [strategies, setStrategies] = useState<Strategy[]>([]);
  const [summary, setSummary] = useState<StrategySummary | null>(null);
  const [loading, setLoading] = useState(false);
  const [filterEnabled, setFilterEnabled] = useState<boolean | null>(null);
  const [filterActive, setFilterActive] = useState<boolean | null>(null);
  const [configModalStrategy, setConfigModalStrategy] = useState<Strategy | null>(null);
  const [updating, setUpdating] = useState<Record<number, boolean>>({});

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

  // 更新策略状态
  const updateStrategyStatus = async (strategyId: number, updates: { is_enabled?: boolean; is_active?: boolean }) => {
    setUpdating(prev => ({ ...prev, [strategyId]: true }));
    try {
      const response = await fetch(`/api/strategy/${strategyId}/status`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updates),
      });
      const result = await response.json();
      if (result.success) {
        // 更新本地状态
        setStrategies(prev => prev.map(s => 
          s.id === strategyId 
            ? { ...s, ...updates, updated_at: result.data.updated_at }
            : s
        ));
        // 刷新摘要
        refreshData();
      } else {
        alert('更新失败: ' + (result.message || '未知错误'));
      }
    } catch (error) {
      console.error('更新策略状态失败:', error);
      alert('更新失败: ' + (error instanceof Error ? error.message : '未知错误'));
    } finally {
      setUpdating(prev => ({ ...prev, [strategyId]: false }));
    }
  };

  // 更新策略配置
  const updateStrategyConfig = async (config: Record<string, any>) => {
    if (!configModalStrategy) return;
    setUpdating(prev => ({ ...prev, [configModalStrategy.id]: true }));
    try {
      const response = await fetch(`/api/strategy/${configModalStrategy.id}/config`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ config }),
      });
      const result = await response.json();
      if (result.success) {
        // 更新本地状态
        setStrategies(prev => prev.map(s => 
          s.id === configModalStrategy.id 
            ? { ...s, config: result.data.config, updated_at: result.data.updated_at }
            : s
        ));
        setConfigModalStrategy(null);
      } else {
        alert('更新配置失败: ' + (result.message || '未知错误'));
      }
    } catch (error) {
      console.error('更新策略配置失败:', error);
      alert('更新配置失败: ' + (error instanceof Error ? error.message : '未知错误'));
    } finally {
      setUpdating(prev => ({ ...prev, [configModalStrategy.id]: false }));
    }
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
                <th style={{ padding: '10px', border: '1px solid #ddd', textAlign: 'left' }}>
                  操作
                </th>
              </tr>
            </thead>
            <tbody>
              {strategies.length === 0 ? (
                <tr>
                  <td
                    colSpan={8}
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
                strategies.map((strategy) => {
                  const isUpdating = updating[strategy.id] || false;
                  return (
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
                      <td style={{ padding: '10px', border: '1px solid #ddd' }}>
                        <div style={{ display: 'flex', gap: '5px', flexWrap: 'wrap' }}>
                          <button
                            onClick={() => updateStrategyStatus(strategy.id, { is_enabled: !strategy.is_enabled })}
                            disabled={isUpdating}
                            style={{
                              padding: '4px 8px',
                              fontSize: '12px',
                              background: strategy.is_enabled ? '#ff4d4f' : '#52c41a',
                              color: '#fff',
                              border: 'none',
                              borderRadius: '4px',
                              cursor: isUpdating ? 'not-allowed' : 'pointer',
                              opacity: isUpdating ? 0.6 : 1,
                            }}
                            title={strategy.is_enabled ? '禁用' : '启用'}
                          >
                            {strategy.is_enabled ? '禁用' : '启用'}
                          </button>
                          <button
                            onClick={() => updateStrategyStatus(strategy.id, { is_active: !strategy.is_active })}
                            disabled={isUpdating}
                            style={{
                              padding: '4px 8px',
                              fontSize: '12px',
                              background: strategy.is_active ? '#faad14' : '#1890ff',
                              color: '#fff',
                              border: 'none',
                              borderRadius: '4px',
                              cursor: isUpdating ? 'not-allowed' : 'pointer',
                              opacity: isUpdating ? 0.6 : 1,
                            }}
                            title={strategy.is_active ? '取消激活' : '激活'}
                          >
                            {strategy.is_active ? '取消激活' : '激活'}
                          </button>
                          <button
                            onClick={() => setConfigModalStrategy(strategy)}
                            disabled={isUpdating}
                            style={{
                              padding: '4px 8px',
                              fontSize: '12px',
                              background: '#722ed1',
                              color: '#fff',
                              border: 'none',
                              borderRadius: '4px',
                              cursor: isUpdating ? 'not-allowed' : 'pointer',
                              opacity: isUpdating ? 0.6 : 1,
                            }}
                            title="编辑配置"
                          >
                            配置
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      )}

      {/* 配置编辑模态框 */}
      <ConfigModal
        strategy={configModalStrategy}
        isOpen={configModalStrategy !== null}
        onClose={() => setConfigModalStrategy(null)}
        onSave={updateStrategyConfig}
      />
    </div>
  );
}

