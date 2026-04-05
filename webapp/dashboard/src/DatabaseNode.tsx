import { useState, useCallback, type FC } from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import type { TableNodeData, ColumnInfo, SampleRow } from './initialElements';

/* ─── tiny icons (inline SVG to avoid deps) ─── */
const KeyIcon = () => (
    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
        <path d="M21 2l-2 2m-7.61 7.61a5.5 5.5 0 1 1-7.778 7.778 5.5 5.5 0 0 1 7.778-7.778zm0 0L15.5 7.5m0 0l3 3L22 7l-3-3m-3.5 3.5L19 4" />
    </svg>
);

const LinkIcon = () => (
    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
        <path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71" />
        <path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71" />
    </svg>
);

const EyeIcon = () => (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
        <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z" />
        <circle cx="12" cy="12" r="3" />
    </svg>
);

const CloseIcon = () => (
    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
        <line x1="18" y1="6" x2="6" y2="18" /><line x1="6" y1="6" x2="18" y2="18" />
    </svg>
);


/* ─── Column row component ─── */
const ColumnRow: FC<{ col: ColumnInfo; expanded: boolean }> = ({ col, expanded }) => {
    // If not expanded, only show primary keys
    if (!expanded && !col.isPrimaryKey) return null;

    return (
        <div className={`db-col-row ${col.isPrimaryKey ? 'pk' : ''} ${col.isForeignKey ? 'fk' : ''}`}>
            <span className="db-col-icon">
                {col.isPrimaryKey && <KeyIcon />}
                {col.isForeignKey && !col.isPrimaryKey && <LinkIcon />}
            </span>
            <span className={`db-col-name ${col.isPrimaryKey && expanded ? 'pk-highlight' : ''}`}>
                {col.name}
            </span>
            <span className="db-col-type">{col.type}</span>
        </div>
    );
};


/* ─── Sample-data preview tab ─── */
const SampleDataTab: FC<{ rows: SampleRow[]; columns: ColumnInfo[]; onClose: () => void }> = ({ rows, columns, onClose }) => {
    const visibleCols = columns.slice(0, 5); // limit width
    return (
        <div className="db-sample-tab">
            <div className="db-sample-header">
                <span>Sample Data</span>
                <button className="db-sample-close" onClick={onClose}><CloseIcon /></button>
            </div>
            <div className="db-sample-scroll">
                <table>
                    <thead>
                        <tr>{visibleCols.map(c => <th key={c.name}>{c.name}</th>)}</tr>
                    </thead>
                    <tbody>
                        {rows.map((row, i) => (
                            <tr key={i}>{visibleCols.map(c => <td key={c.name}>{String(row[c.name] ?? '—')}</td>)}</tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
};


/* ═══════════════════════════════════════════════════════
   DatabaseNode – the main custom node
   ═══════════════════════════════════════════════════════ */
const DatabaseNode: FC<NodeProps> = ({ data }) => {
    const { label, columns, sampleData } = data as TableNodeData;
    const [hovered, setHovered] = useState(false);
    const [showSample, setShowSample] = useState(false);

    const onMouseEnter = useCallback(() => setHovered(true), []);
    const onMouseLeave = useCallback(() => setHovered(false), []);

    const primaryKeys = columns.filter((c: ColumnInfo) => c.isPrimaryKey);
    const columnCount = columns.length;

    return (
        <div
            className={`db-node ${hovered ? 'db-node--hovered' : ''}`}
            onMouseEnter={onMouseEnter}
            onMouseLeave={onMouseLeave}
        >
            {/* Incoming handle (top) */}
            <Handle type="target" position={Position.Top} className="db-handle" />

            {/* ── Header ── */}
            <div className="db-node-header">
                <div className="db-node-title">{label}</div>
                <span className="db-node-badge">{columnCount} cols</span>
            </div>

            {/* ── Columns section ── */}
            <div className="db-node-columns">
                {!hovered && (
                    <>
                        {primaryKeys.map((col: ColumnInfo) => (
                            <ColumnRow key={col.name} col={col} expanded={false} />
                        ))}
                        <div className="db-col-hint">
                            hover to see all {columnCount} columns
                        </div>
                    </>
                )}
                {hovered && (
                    <>
                        {columns.map((col: ColumnInfo) => (
                            <ColumnRow key={col.name} col={col} expanded={true} />
                        ))}
                    </>
                )}
            </div>

            {/* ── Preview button (appears on hover) ── */}
            {hovered && !showSample && (
                <button className="db-preview-btn" onClick={() => setShowSample(true)}>
                    <EyeIcon /> Preview Data
                </button>
            )}

            {/* ── Sample data tab ── */}
            {showSample && (
                <SampleDataTab rows={sampleData} columns={columns} onClose={() => setShowSample(false)} />
            )}

            {/* Outgoing handle (bottom) */}
            <Handle type="source" position={Position.Bottom} className="db-handle" />
        </div>
    );
};

export default DatabaseNode;
