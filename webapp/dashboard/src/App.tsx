import { useState, useCallback } from 'react';
import '@xyflow/react/dist/style.css';

import CrudPanel from './CrudPanel';
// import DatabaseNode from './DatabaseNode';
// import { fetchSchema } from './api';
// import { type Node, type Edge, MarkerType, type NodeTypes } from '@xyflow/react';

/* Register our custom node type */
// const nodeTypes: NodeTypes = {
//   databaseTable: DatabaseNode,
// };

type View = 'crud';

/* ─── Icon components for sidebar ─── */
// const SchemaIcon = () => (
//   <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
//     <rect x="3" y="3" width="18" height="18" rx="2" ry="2" />
//     <line x1="3" y1="9" x2="21" y2="9" />
//     <line x1="9" y1="21" x2="9" y2="9" />
//   </svg>
// );

const CrudIcon = () => (
  <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
    <polyline points="14 2 14 8 20 8" />
    <line x1="12" y1="18" x2="12" y2="12" />
    <line x1="9" y1="15" x2="15" y2="15" />
  </svg>
);

export default function App() {
  const [view] = useState<View>('crud');
  const [refreshKey, setRefreshKey] = useState(0);
  /* Schema loading and ReactFlow rendering disabled by request.
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [selectedTable, setSelectedTable] = useState<string>('');
  const [allTableNames, setAllTableNames] = useState<string[]>([]);
  const [schemaLoading, setSchemaLoading] = useState(true);

  const loadSchema = useCallback(async () => {
    // ... existing schema loading logic kept in git history
  }, [setNodes, setEdges, selectedTable]);

  useEffect(() => {
    loadSchema();
  }, [loadSchema, refreshKey]);

  const onInit = useCallback(() => {
    console.log('[ChiralDB] Dashboard initialised');
  }, []);
  */

  const handleDataChanged = useCallback(() => {
    setRefreshKey(prev => prev + 1);
  }, []);

  return (
    <div className="app-shell">
      {/* ── Sidebar ── */}
      <nav className="app-sidebar">
        <div className="sidebar-brand">
          <span className="app-logo">◈</span>
        </div>
        <div className="sidebar-nav">
          {/* Schema switch hidden by request.
          <button
            className={`sidebar-btn ${view === 'schema' ? 'active' : ''}`}
            onClick={() => setView('schema')}
            title="Schema View"
          >
            <SchemaIcon />
            <span className="sidebar-label">Schema</span>
          </button>
          */}
          <button
            className={`sidebar-btn ${view === 'crud' ? 'active' : ''}`}
            title="CRUD Operations"
          >
            <CrudIcon />
            <span className="sidebar-label">CRUD</span>
          </button>
        </div>
      </nav>

      {/* ── Main content area ── */}
      <div className="app-main">
        {/* ── Top bar ── */}
        <header className="app-header">
          <div className="app-brand">
            <h1>Chiral<span className="brand-accent">DB</span></h1>
          </div>
          <p className="app-subtitle">Query Executor & CRUD Operations</p>
          <div className="header-status">
            {/* Schema table dropdown hidden by request.
            {view === 'schema' && allTableNames.length > 0 && (
              <div className="schema-dropdown-container">
                <span className="schema-dropdown-label">Explore Table:</span>
                <select
                  className="schema-dropdown"
                  value={selectedTable || (allTableNames.includes('chiral_data') ? 'chiral_data' : allTableNames[0])}
                  onChange={(e) => setSelectedTable(e.target.value)}
                >
                  {allTableNames.map(t => (
                    <option key={t} value={t}>{t}</option>
                  ))}
                </select>
              </div>
            )}
            */}
            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
              <span className={`status-dot ${refreshKey > 0 ? 'status-dot--active' : ''}`} />
              <span className="status-text">Live</span>
            </div>
          </div>
        </header>

        {/* Schema view disabled by request.
        {view === 'schema' && (
          <>
            <div className="flow-container" key={`flow-${refreshKey}`}>...</div>
            <footer className="app-legend">...</footer>
          </>
        )}
        */}

        {/* ── CRUD view ── */}
        {view === 'crud' && (
          <CrudPanel onDataChanged={handleDataChanged} />
        )}
      </div>
    </div>
  );
}
