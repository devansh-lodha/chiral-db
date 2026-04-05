const API_BASE = 'http://127.0.0.1:8000';

export interface QueryRequest {
    operation: 'read' | 'create' | 'update' | 'delete';
    table?: string;
    session_id?: string;
    select?: string[];
    filters?: { field: string; op: string; value: unknown }[];
    payload?: Record<string, unknown>;
    updates?: Record<string, unknown>;
    limit?: number;
    offset?: number;
}

export interface QueryResponse {
    sql?: string;
    params?: Record<string, unknown>;
    rows?: Record<string, unknown>[];
    row_count?: number;
    affected_rows?: number;
    mode?: string;
    error?: string;
}

export async function executeQuery(request: QueryRequest): Promise<QueryResponse> {
    const res = await fetch(`${API_BASE}/query/execute`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(request),
    });
    if (!res.ok) {
        const errBody = await res.json().catch(() => ({ detail: res.statusText }));
        throw new Error(typeof errBody.detail === 'string' ? errBody.detail : JSON.stringify(errBody.detail));
    }
    return res.json();
}

export async function fetchLogicalFields(sessionId: string): Promise<string[]> {
    const res = await fetch(`${API_BASE}/schema/logical/${sessionId}`);
    if (!res.ok) throw new Error('Failed to fetch logical fields');
    return res.json();
}

export async function fetchSessions(): Promise<string[]> {
    const res = await fetch(`${API_BASE}/sessions/active`);
    if (!res.ok) throw new Error('Failed to fetch active sessions');
    return res.json();
}

export interface SessionInfo {
    session_id: string;
    record_count: number;
    status: string;
    schema_version: number;
}

export async function fetchSessionInfo(sessionId: string): Promise<SessionInfo> {
    const req: QueryRequest = {
        operation: 'read',
        table: 'session_metadata',
        filters:[{ field: 'session_id', op: 'eq', value: sessionId }]
    };
    const res = await executeQuery(req);
    if (res.rows && res.rows.length > 0) {
        return res.rows[0] as unknown as SessionInfo;
    }
    throw new Error('Session not found');
}

export async function flushSession(sessionId: string): Promise<Record<string, unknown>> {
    const res = await fetch(`${API_BASE}/flush/${sessionId}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
    });
    if (!res.ok) {
        const errBody = await res.json().catch(() => ({ detail: res.statusText }));
        throw new Error(typeof errBody.detail === 'string' ? errBody.detail : JSON.stringify(errBody.detail));
    }
    return res.json();
}
