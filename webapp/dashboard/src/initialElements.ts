import { type Node, type Edge, MarkerType } from '@xyflow/react';

// ─────────────────────────────────────────────────────────
// Types for our custom node data
// ─────────────────────────────────────────────────────────
export interface ColumnInfo {
    name: string;
    type: string;
    isPrimaryKey: boolean;
    isForeignKey: boolean;
    foreignRef?: string;          // e.g. "session_metadata.session_id"
}

export interface SampleRow {
    [key: string]: string | number | boolean | null;
}

export interface TableNodeData {
    label: string;
    columns: ColumnInfo[];
    sampleData: SampleRow[];
    [key: string]: unknown;
}

// ─────────────────────────────────────────────────────────
// Schema definitions matching the actual chiral-db schema
// ─────────────────────────────────────────────────────────

const sessionMetadataColumns: ColumnInfo[] = [
    { name: 'session_id', type: 'TEXT', isPrimaryKey: true, isForeignKey: false },
    { name: 'record_count', type: 'INTEGER', isPrimaryKey: false, isForeignKey: false },
    { name: 'status', type: 'TEXT', isPrimaryKey: false, isForeignKey: false },
    { name: 'schema_json', type: 'TEXT', isPrimaryKey: false, isForeignKey: false },
    { name: 'schema_version', type: 'INTEGER', isPrimaryKey: false, isForeignKey: false },
    { name: 'drift_events', type: 'JSONB', isPrimaryKey: false, isForeignKey: false },
    { name: 'safety_events', type: 'JSONB', isPrimaryKey: false, isForeignKey: false },
    { name: 'migration_metrics', type: 'JSONB', isPrimaryKey: false, isForeignKey: false },
    { name: 'created_at', type: 'TIMESTAMP', isPrimaryKey: false, isForeignKey: false },
];

const chiralDataColumns: ColumnInfo[] = [
    { name: 'id', type: 'SERIAL', isPrimaryKey: true, isForeignKey: false },
    { name: 'session_id', type: 'TEXT', isPrimaryKey: false, isForeignKey: true, foreignRef: 'session_metadata.session_id' },
    { name: 'username', type: 'TEXT', isPrimaryKey: false, isForeignKey: false },
    { name: 'sys_ingested_at', type: 'FLOAT', isPrimaryKey: false, isForeignKey: false },
    { name: 't_stamp', type: 'FLOAT', isPrimaryKey: false, isForeignKey: false },
    { name: 'overflow_data', type: 'JSONB', isPrimaryKey: false, isForeignKey: false },
];

const stagingDataColumns: ColumnInfo[] = [
    { name: 'id', type: 'SERIAL', isPrimaryKey: true, isForeignKey: false },
    { name: 'session_id', type: 'TEXT', isPrimaryKey: false, isForeignKey: true, foreignRef: 'session_metadata.session_id' },
    { name: 'data', type: 'JSONB', isPrimaryKey: false, isForeignKey: false },
];

const commentsColumns: ColumnInfo[] = [
    { name: 'id', type: 'SERIAL', isPrimaryKey: true, isForeignKey: false },
    { name: 'chiral_data_id', type: 'INTEGER', isPrimaryKey: false, isForeignKey: true, foreignRef: 'chiral_data.id' },
    { name: 'session_id', type: 'TEXT', isPrimaryKey: false, isForeignKey: true, foreignRef: 'session_metadata.session_id' },
    { name: 'comment_id', type: 'INTEGER', isPrimaryKey: false, isForeignKey: false },
    { name: 'text', type: 'TEXT', isPrimaryKey: false, isForeignKey: false },
    { name: 'score', type: 'DOUBLE PRECISION', isPrimaryKey: false, isForeignKey: false },
    { name: 'is_flagged', type: 'BOOLEAN', isPrimaryKey: false, isForeignKey: false },
    { name: 'overflow_data', type: 'JSONB', isPrimaryKey: false, isForeignKey: false },
];

const eventsColumns: ColumnInfo[] = [
    { name: 'id', type: 'SERIAL', isPrimaryKey: true, isForeignKey: false },
    { name: 'chiral_data_id', type: 'INTEGER', isPrimaryKey: false, isForeignKey: true, foreignRef: 'chiral_data.id' },
    { name: 'session_id', type: 'TEXT', isPrimaryKey: false, isForeignKey: true, foreignRef: 'session_metadata.session_id' },
    { name: 'event_id', type: 'INTEGER', isPrimaryKey: false, isForeignKey: false },
    { name: 'event_type', type: 'TEXT', isPrimaryKey: false, isForeignKey: false },
    { name: 'amount', type: 'DOUBLE PRECISION', isPrimaryKey: false, isForeignKey: false },
    { name: 'is_conversion', type: 'BOOLEAN', isPrimaryKey: false, isForeignKey: false },
    { name: 'overflow_data', type: 'JSONB', isPrimaryKey: false, isForeignKey: false },
];

// ─────────────────────────────────────────────────────────
// Sample data for the "preview" tabs
// ─────────────────────────────────────────────────────────

const sessionSample: SampleRow[] = [
    { session_id: 'session_assignment_2', record_count: 1000, status: 'finalised', schema_version: 3 },
    { session_id: 'session_test_01', record_count: 250, status: 'collecting', schema_version: 1 },
    { session_id: 'session_demo', record_count: 500, status: 'finalised', schema_version: 2 },
];

const chiralSample: SampleRow[] = [
    { id: 1, session_id: 'session_assignment_2', username: 'user_0', sys_ingested_at: 1711574400.0, t_stamp: 1711574400.0 },
    { id: 2, session_id: 'session_assignment_2', username: 'user_1', sys_ingested_at: 1711574401.5, t_stamp: 1711574401.5 },
    { id: 3, session_id: 'session_assignment_2', username: 'user_2', sys_ingested_at: 1711574403.2, t_stamp: 1711574403.2 },
];

const stagingSample: SampleRow[] = [
    { id: 1, session_id: 'session_assignment_2', data: '{"username":"user_0","city":"Paris"}' },
    { id: 2, session_id: 'session_assignment_2', data: '{"username":"user_1","city":"Berlin"}' },
];

const commentsSample: SampleRow[] = [
    { id: 1, chiral_data_id: 1, comment_id: 10, text: 'comment-10', score: 0.541, is_flagged: false },
    { id: 2, chiral_data_id: 1, comment_id: 11, text: 'comment-11', score: 0.873, is_flagged: true },
    { id: 3, chiral_data_id: 2, comment_id: 20, text: 'comment-20', score: 0.322, is_flagged: false },
];

const eventsSample: SampleRow[] = [
    { id: 1, chiral_data_id: 1, event_id: 10, event_type: 'click', amount: 89.99, is_conversion: true },
    { id: 2, chiral_data_id: 1, event_id: 11, event_type: 'view', amount: 12.50, is_conversion: false },
    { id: 3, chiral_data_id: 2, event_id: 20, event_type: 'purchase', amount: 249.00, is_conversion: true },
];

// ─────────────────────────────────────────────────────────
// React Flow nodes
// ─────────────────────────────────────────────────────────

export const initialNodes: Node<TableNodeData>[] = [
    {
        id: 'session_metadata',
        type: 'databaseTable',
        position: { x: 400, y: 0 },
        data: { label: 'session_metadata', columns: sessionMetadataColumns, sampleData: sessionSample },
    },
    {
        id: 'chiral_data',
        type: 'databaseTable',
        position: { x: 200, y: 320 },
        data: { label: 'chiral_data', columns: chiralDataColumns, sampleData: chiralSample },
    },
    {
        id: 'staging_data',
        type: 'databaseTable',
        position: { x: 700, y: 320 },
        data: { label: 'staging_data', columns: stagingDataColumns, sampleData: stagingSample },
    },
    {
        id: 'chiral_data_comments',
        type: 'databaseTable',
        position: { x: 0, y: 640 },
        data: { label: 'chiral_data_comments', columns: commentsColumns, sampleData: commentsSample },
    },
    {
        id: 'chiral_data_events',
        type: 'databaseTable',
        position: { x: 400, y: 640 },
        data: { label: 'chiral_data_events', columns: eventsColumns, sampleData: eventsSample },
    },
];

// ─────────────────────────────────────────────────────────
// React Flow edges (parent → child, directed arrows)
// ─────────────────────────────────────────────────────────

const edgeDefaults = {
    animated: true,
    style: { stroke: '#6366f1', strokeWidth: 2, strokeDasharray: '5 5' },
    markerEnd: { type: MarkerType.ArrowClosed, color: '#6366f1', width: 20, height: 20 },
};

export const initialEdges: Edge[] = [
    { id: 'e-session-chiral', source: 'session_metadata', target: 'chiral_data', ...edgeDefaults },
    { id: 'e-session-staging', source: 'session_metadata', target: 'staging_data', ...edgeDefaults },
    { id: 'e-chiral-comments', source: 'chiral_data', target: 'chiral_data_comments', ...edgeDefaults },
    { id: 'e-chiral-events', source: 'chiral_data', target: 'chiral_data_events', ...edgeDefaults },
];
