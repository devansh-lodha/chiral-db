import React, { useState, useEffect, useCallback } from 'react';
import { executeQuery, fetchLogicalFields, fetchSessions, fetchSessionInfo } from './api';
import type { QueryRequest, QueryResponse, SessionInfo } from './api';
import SearchableDropdown from './SearchableDropdown';

const DEFAULT_PAYLOAD = `{
  "username": "deep_testing",
  "city": "Gujarat",
  "comments":[
      { "text": "hello" }
  ]
}`;

type Operation = 'read' | 'create' | 'update' | 'delete';

interface ResultTab {
    id: number;
    label: string;
    response: QueryResponse;
    request: QueryRequest;
    timestamp: Date;
}
let tabCounter = 0;

const JsonCell: React.FC<{ data: unknown }> = ({ data }) => {
    const[expanded, setExpanded] = useState(false);
    if (typeof data !== 'object' || data === null) return <>{String(data)}</>;
    const jsonString = JSON.stringify(data, null, 2);
    if (jsonString.length <= 40) return <span className="jsonb-preview">{jsonString}</span>;
    return (
        <div className="jsonb-cell">
            <button className="jsonb-toggle" onClick={(e) => { e.stopPropagation(); setExpanded(!expanded); }}>
                {expanded ? '▲ Hide' : '▼ Show'} JSON
            </button>
            {!expanded && <span className="jsonb-preview">{"{...}"}</span>}
            {expanded && <div className="jsonb-expanded">{jsonString}</div>}
        </div>
    );
};

const CrudPanel: React.FC<{ onDataChanged?: () => void }> = ({ onDataChanged }) => {
    const [operation, setOperation] = useState<Operation>('read');
    
    // Dynamic Metadata State
    const [sessionIds, setSessionIds] = useState<string[]>([]);
    const [sessionId, setSessionId] = useState('');
    const[sessionInfo, setSessionInfo] = useState<SessionInfo | null>(null);
    const [logicalFields, setLogicalFields] = useState<string[]>([]);
    
    const [selectFields, setSelectFields] = useState<string[]>([]);
    const [filterField, setFilterField] = useState('');
    const [filterOp, setFilterOp] = useState('eq');
    const [filterValue, setFilterValue] = useState('');
    const[limit, setLimit] = useState<number | string>(10);
    const[payloadJson, setPayloadJson] = useState(DEFAULT_PAYLOAD);
    
    // Key-Value builder state for Updates (No more JSON!)
    const [updateKVs, setUpdateKVs] = useState<{field: string, value: string}[]>([{field: '', value: ''}]);

    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [resultTabs, setResultTabs] = useState<ResultTab[]>([]);
    const [activeTabId, setActiveTabId] = useState<number | null>(null);

    // Initial Load
    useEffect(() => {
        fetchSessions().then(ids => {
            setSessionIds(ids);
            if (ids.length > 0) setSessionId(ids[0]);
        }).catch(err => console.error("Session load error:", err));
    },[]);

    // Load Metadata when Session changes
    useEffect(() => {
        if (!sessionId) return;
        fetchLogicalFields(sessionId).then(setLogicalFields).catch(err => console.error("Field load error:", err));
        fetchSessionInfo(sessionId).then(setSessionInfo).catch(() => setSessionInfo(null));
    },[sessionId]);

    const handleUpdateChange = (index: number, key: 'field'|'value', val: string) => {
        setUpdateKVs(prev => {
            const newKVs = [...prev];
            newKVs[index] = { ...newKVs[index], [key]: val };
            return newKVs;
        });
    };

    const executeOp = useCallback(async () => {
        setError(null);
        setLoading(true);

        const req: QueryRequest = { operation, table: 'chiral_data', session_id: sessionId };

        if (operation === 'read') {
            if (selectFields.length === 0) {
                setError('Please select at least one field to read.');
                setLoading(false); return;
            }
            req.select = selectFields;
            if (limit) req.limit = typeof limit === 'string' ? parseInt(limit, 10) : limit;
        }

        if (filterField && filterValue) {
            let val: string | number | boolean = filterValue;
            if (!isNaN(Number(val)) && val.trim() !== '') val = Number(val);
            else if (val.toLowerCase() === 'true') val = true;
            else if (val.toLowerCase() === 'false') val = false;
            req.filters = [{ field: filterField, op: filterOp, value: val }];
        }

        if (operation === 'create') {
            try { req.payload = JSON.parse(payloadJson) as Record<string, unknown>; } 
            catch { setError('Invalid JSON payload for CREATE'); setLoading(false); return; }
        }

        if (operation === 'update') {
            const updates: Record<string, string | number | boolean> = {};
            updateKVs.forEach(kv => {
                if (kv.field && kv.value) {
                    let val: string | number | boolean = kv.value;
                    if (!isNaN(Number(val)) && val.trim() !== '') val = Number(val);
                    else if (val.toLowerCase() === 'true') val = true;
                    else if (val.toLowerCase() === 'false') val = false;
                    updates[kv.field] = val;
                }
            });
            if (Object.keys(updates).length === 0) {
                setError('Please provide at least one update field and value.');
                setLoading(false); return;
            }
            req.updates = updates;
        }

        try {
            const response = await executeQuery(req);
            const tabLabel = `${operation.toUpperCase()} LOGICAL`;
            const newTab: ResultTab = { id: ++tabCounter, label: tabLabel, response, request: req, timestamp: new Date() };
            setResultTabs(prev => [...prev, newTab]);
            setActiveTabId(newTab.id);

            // Refresh metadata to reflect new rows
            fetchSessionInfo(sessionId).then(setSessionInfo);

            if (operation !== 'read' && onDataChanged) {
                onDataChanged();
                fetchLogicalFields(sessionId).then(setLogicalFields);
            }
        } catch (err: unknown) {
            setError(err instanceof Error ? err.message : 'Unknown error');
        } finally {
            setLoading(false);
        }
    },[operation, sessionId, selectFields, filterField, filterOp, filterValue, payloadJson, updateKVs, limit, onDataChanged]);

    const activeTab = resultTabs.find(t => t.id === activeTabId);

    return (
        <div className="crud-panel">
            <div className="crud-form-container" style={{ width: 400 }}>
                <div className="crud-form">
                    <h2 className="crud-title">Logical Operations</h2>

                    <div className="crud-row">
                        <div className="crud-op-selector">
                            {(['read', 'create', 'update', 'delete'] as Operation[]).map(op => (
                                <button key={op} className={`crud-op-btn ${operation === op ? 'active' : ''} crud-op-${op}`} onClick={() => setOperation(op)}>
                                    {op.toUpperCase()}
                                </button>
                            ))}
                        </div>
                    </div>

                    <div className="crud-row">
                        <label>Session Context</label>
                        <SearchableDropdown options={sessionIds} value={sessionId} onChange={(v) => setSessionId(v as string)} placeholder="Select session..." allowFreeText />
                        
                        {/* Dynamic Session Status Widget */}
                        {sessionInfo && (
                            <div style={{ fontSize: '11px', color: 'var(--text-muted)', marginTop: '6px', padding: '6px 10px', background: 'var(--bg-muted)', borderRadius: '6px', display: 'flex', gap: '14px', border: '1px solid var(--border)' }}>
                                <span>Status: <b style={{color: 'var(--accent)'}}>{String(sessionInfo.status).toUpperCase()}</b></span>
                                <span>Records: <b style={{color: 'var(--text-primary)'}}>{sessionInfo.record_count}</b></span>
                                <span>Schema: <b style={{color: 'var(--text-primary)'}}>v{sessionInfo.schema_version}</b></span>
                            </div>
                        )}
                    </div>

                    {operation === 'read' && (
                        <>
                            <div className="crud-row">
                                <label>Select Fields</label>
                                <SearchableDropdown options={logicalFields} value={selectFields} onChange={(v) => setSelectFields(v as string[])} placeholder="Select fields..." multiple />
                            </div>
                            <div className="crud-row">
                                <label>Limit</label>
                                <input className="crud-input" type="number" value={limit} onChange={e => setLimit(e.target.value)} />
                            </div>
                        </>
                    )}

                    <div className="crud-row">
                        <label>Target Filter</label>
                        <div className="crud-filter-row">
                            <SearchableDropdown options={logicalFields} value={filterField} onChange={(v) => setFilterField(v as string)} placeholder="Field..." allowFreeText />
                            <select value={filterOp} onChange={e => setFilterOp(e.target.value)} className="crud-select crud-select-sm">
                                <option value="eq">=</option>
                                <option value="neq">≠</option>
                                <option value="gt">&gt;</option>
                                <option value="gte">≥</option>
                                <option value="lt">&lt;</option>
                                <option value="lte">≤</option>
                            </select>
                            <input className="crud-input crud-input-sm" value={filterValue} onChange={e => setFilterValue(e.target.value)} placeholder="Value..." />
                        </div>
                    </div>

                    {operation === 'create' && (
                        <div className="crud-row">
                            <label>Payload (JSON)</label>
                            <textarea className="crud-textarea" style={{minHeight: '150px'}} value={payloadJson} onChange={e => setPayloadJson(e.target.value)} />
                        </div>
                    )}

                    {operation === 'update' && (
                        <div className="crud-row">
                            <label>Logical Updates</label>
                            {updateKVs.map((kv, i) => (
                                <div key={i} className="crud-filter-row" style={{marginBottom: '6px'}}>
                                    <SearchableDropdown options={logicalFields} value={kv.field} onChange={(v) => handleUpdateChange(i, 'field', v as string)} placeholder="Field" allowFreeText />
                                    <input className="crud-input crud-input-sm" value={kv.value} onChange={(e) => handleUpdateChange(i, 'value', e.target.value)} placeholder="New Value" />
                                    {updateKVs.length > 1 && (
                                        <button style={{background:'transparent', border:'none', color:'var(--danger-color)', cursor:'pointer', padding:'0 4px', fontSize: '16px', fontWeight: 'bold'}} onClick={() => setUpdateKVs(updateKVs.filter((_, idx) => idx !== i))}>×</button>
                                    )}
                                </div>
                            ))}
                            <button onClick={() => setUpdateKVs([...updateKVs, {field: '', value: ''}])} style={{alignSelf: 'flex-start', background: 'var(--bg-muted)', border: '1px solid var(--border)', borderRadius: '4px', fontSize: '11px', padding: '4px 8px', cursor: 'pointer', color: 'var(--text-secondary)'}}>+ Add Field</button>
                        </div>
                    )}

                    <button className="crud-submit-btn" onClick={() => { executeOp(); }} disabled={loading} style={{marginTop: 'auto'}}>
                        {loading ? 'Executing...' : `Execute ${operation.toUpperCase()}`}
                    </button>
                </div>
            </div>

            <div className="crud-results">
                {error && (
                    <div className="crud-error-banner" style={{margin: '16px'}}>
                        {error} <button className="crud-error-dismiss" onClick={() => setError(null)}>✕</button>
                    </div>
                )}
                <div className="crud-tabs-header">
                    {resultTabs.map(tab => (
                        <div key={tab.id} className={`crud-tab ${activeTabId === tab.id ? 'active' : ''}`} onClick={() => setActiveTabId(tab.id)}>
                            {tab.label}
                            <span className="crud-tab-close" onClick={(e) => { e.stopPropagation(); setResultTabs(prev => prev.filter(t => t.id !== tab.id)); }}>✕</span>
                        </div>
                    ))}
                </div>

                {activeTab ? (
                    <div className="crud-tab-content crud-result-view">
                        <div className="crud-result-meta">
                            <span className="crud-result-time">⏱ {activeTab.timestamp.toLocaleTimeString()}</span>
                            {activeTab.response.row_count !== undefined && <span className="crud-result-badge crud-result-count">{activeTab.response.row_count} rows read</span>}
                            {activeTab.response.affected_rows !== undefined && <span className="crud-result-badge">{activeTab.response.affected_rows} rows written</span>}
                        </div>

                        {activeTab.response.rows && activeTab.response.rows.length > 0 && (
                            <div className="crud-data-scroll">
                                <table className="crud-data-table">
                                    <thead><tr>{Object.keys(activeTab.response.rows[0]).map(k => <th key={k}>{k}</th>)}</tr></thead>
                                    <tbody>
                                        {activeTab.response.rows.map((row: Record<string, unknown>, i: number) => (
                                            <tr key={i}>
                                                {Object.keys(activeTab.response.rows![0]).map(k => (
                                                    <td key={k}>{typeof row[k] === 'object' ? <JsonCell data={row[k]} /> : String(row[k])}</td>
                                                ))}
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        )}
                        
                        {/* Hidden Physical Debug Info */}
                        <details style={{marginTop: '20px', cursor: 'pointer', color: 'var(--text-muted)', fontSize: '12px'}}>
                            <summary>View SQL Execution Details</summary>
                            <div className="crud-sql-block" style={{ marginTop: '12px', padding: '12px' }}>
                                <pre>{activeTab.response.sql}</pre>
                                <hr style={{margin: '10px 0', border: 'none', borderTop: '1px solid var(--border)'}}/>
                                <pre>{JSON.stringify(activeTab.response.params, null, 2)}</pre>
                            </div>
                        </details>
                    </div>
                ) : (
                    <div className="crud-tab-content-empty">Execute an operation to see results.</div>
                )}
            </div>
        </div>
    );
};
export default CrudPanel;