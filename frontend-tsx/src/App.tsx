import React, { useState, useEffect } from 'react';
import { supabase } from './supabase';
import type { Session } from '@supabase/supabase-js';

import './index.css';

interface Config {
  coord: string; endpoint: string; key: string; secret: string; bucket: string; workers: number; secrets?: any[];
}

interface LogEntry { ts: string; msg: string; type: 'ok' | 'err' | 'info'; }

export default function App() {
  const [session, setSession] = useState<Session | null>(null);
  const [authLoading, setAuthLoading] = useState(true);
  const [authMode, setAuthMode] = useState<'login' | 'signup'>('login');
  const [authEmail, setAuthEmail] = useState('');
  const [authPassword, setAuthPassword] = useState('');
  const [authError, setAuthError] = useState('');

  const [section, setSection] = useState('query');
  const [cfg, setCfg] = useState<Config>({
    coord: 'https://duckpond-coordinator.fly.dev', endpoint: 'https://fly.storage.tigris.dev', key: '', secret: '', bucket: 'duckpond-data', workers: 4
  });

  const [statusOk, setStatusOk] = useState(false);
  const [logs, setLogs] = useState<LogEntry[]>([]);

  const [querySql, setQuerySql] = useState('SELECT col, COUNT(*) AS n\nFROM read_parquet([{files}])\nGROUP BY col\nORDER BY n DESC\nLIMIT 50');
  const [computeTier, setComputeTier] = useState<'micro' | 'standard' | 'enterprise'>('micro');
  const [queryRunning, setQueryRunning] = useState(false);
  const [exportMenuOpen, setExportMenuOpen] = useState(false);
  const [supportForm, setSupportForm] = useState({ name: '', email: '', subject: 'Technical', message: '' });
  const [supportStatus, setSupportStatus] = useState('');
  const [queryStatus, setQueryStatus] = useState('');
  const [results, setResults] = useState<{ columns: string[], rows: any[], row_count: number } | null>(null);

  const [newSecret, setNewSecret] = useState({ name: 'remote_db', type: 'POSTGRES', host: '', user: '', pass: '', port: '5432' });

  const abortCtrl = React.useRef<AbortController | null>(null);

  const abortQuery = () => {
    if (abortCtrl.current) {
      abortCtrl.current.abort();
      abortCtrl.current = null;
      setQueryRunning(false);
      setQueryStatus('Aborted by user.');
      addLog('Query aggressively aborted!', 'err');
    }
  };

  const [schemaTables, setSchemaTables] = useState<any[]>([]);
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [schemaNamespace, setSchemaNamespace] = useState('duckpond');
  const [ingestForm, setIngestForm] = useState({ uri: '', table: '', partCol: '', tier: 'standard' });
  const [ingestStatus, setIngestStatus] = useState('');
  const [uploadTable, setUploadTable] = useState('');
  const [uploadStatus, setUploadStatus] = useState('');
  const [catalogFilter, setCatalogFilter] = useState('');

  // Design System States
  const [themeMode, setThemeMode] = useState<'dark' | 'light'>('dark');
  const [season, setSeason] = useState<'spring' | 'summer' | 'autumn' | 'winter'>('spring');
  const [showLanding, setShowLanding] = useState(true);
  const [showAboutUs, setShowAboutUs] = useState(false);
  const [aboutForm, setAboutForm] = useState({ name: '', email: '', message: '' });
  const [aboutStatus, setAboutStatus] = useState('');

  // Feature Carousel Logic
  const [lastExecutedSql, setLastExecutedSql] = useState('');
  const [featureIndex, setFeatureIndex] = useState(0);
  const featureSets = React.useMemo(() => [
    [
      "Scale globally at origin with purely serverless DuckDB and Apache Iceberg infrastructure.",
      "Workers scale up elastically on demand and sleep when you're done. $0.00 minimum charges.",
      "Horizontal Fan-out. Distribute compute across thousands of lightweight workers."
    ],
    [
      "Zero warehouse stress. Pay for compute, not Scala overhead.",
      "Every model is an Iceberg table on your S3. Portable Parquet, zero lock-in architecture.",
      "Proper REST Catalog support. Seamless integration with Spark, Trino, and Snowflake."
    ],
    [
      "Built-in dbt Runner. Point at a Git repo and we materialize the lakehouse automatically.",
      "Full Browser SQL console. Query instantly with a high-fidelity IDE and catalog browser.",
      "Easily bring external S3 data or connect to other systems. Import .csv or parquet files."
    ],
    [
      "TPC-H verified. Run complex decision-support queries at scale with sub-5s latency.",
      "Battle-tested on SF-100. Fast multi-table joins and correlated subqueries.",
      "Production SQL engine. ACID transactions on Iceberg with DuckDB performance."
    ]
  ], []);

  useEffect(() => {
    if (!showLanding) return;
    const timer = setInterval(() => {
      setFeatureIndex((prev) => (prev + 1) % featureSets.length);
    }, 15000);
    return () => clearInterval(timer);
  }, [showLanding, featureSets.length]);

  useEffect(() => {
    document.body.className = `${themeMode === 'light' ? 'theme-light' : ''} season-${season}`;
  }, [themeMode, season]);

  // Cycle seasons automatically when on the login screen
  useEffect(() => {
    if (session) return;
    const interval = setInterval(() => {
      setSeason(prev => {
        if (prev === 'spring') return 'summer';
        if (prev === 'summer') return 'autumn';
        if (prev === 'autumn') return 'winter';
        return 'spring';
      });
    }, 30000);
    return () => clearInterval(interval);
  }, [session]);

  // SQL Autocomplete state
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [suggestionIdx, setSuggestionIdx] = useState(0);
  const editorRef = React.useRef<HTMLTextAreaElement>(null);

  // Org state
  const [orgs, setOrgs] = useState<any[]>([]);
  const [orgMembers, setOrgMembers] = useState<any[]>([]);
  const [newOrgName, setNewOrgName] = useState('');
  const [newOrgSlug, setNewOrgSlug] = useState('');
  const [inviteEmail, setInviteEmail] = useState('');
  const [orgStatus, setOrgStatus] = useState('');

  // dbt state
  const [dbtCommand, setDbtCommand] = useState('build');
  const [dbtGitUrl, setDbtGitUrl] = useState('https://github.com/dbt-labs/jaffle_shop_duckdb.git');
  const [dbtSource, setDbtSource] = useState<'git' | 'upload'>('git');
  const [_dbtJobId, setDbtJobId] = useState('');
  const [dbtStatus, setDbtStatus] = useState('');
  const [dbtLogs, setDbtLogs] = useState<string[]>([]);
  const [dbtTables, setDbtTables] = useState<string[]>([]);
  const [dbtRunning, setDbtRunning] = useState(false);
  const [dbtJobs, setDbtJobs] = useState<any[]>([]);
  const [exporting, setExporting] = useState(false);

  // Billing UI State
  const [billingProfile, setBillingProfile] = useState<any>(null);
  const [showUpgradeModal, setShowUpgradeModal] = useState(false);

  // Physics Matrix
  const particles = React.useMemo(() => Array.from({ length: 30 }).map(() => ({
    left: Math.random() * 100,
    delay: Math.random() * 10,
    duration: 8 + Math.random() * 12,
    size: 0.5 + Math.random() * 1
  })), []);

  const isTrialExpired = billingProfile && billingProfile.trial_ends_at && new Date(billingProfile.trial_ends_at) < new Date();
  const isOutofCredits = billingProfile && parseFloat(billingProfile.compute_credit_balance) <= 0;
  const isLocked = isTrialExpired || isOutofCredits;

  const addLog = (msg: string, type: 'ok' | 'err' | 'info' = 'info') => {
    setLogs(prev => [...prev, { ts: new Date().toTimeString().slice(0, 8), msg, type }]);
  };

  // ---- Contact Form via Supabase Table ----------------------------------------
  const sendContactEmail = async (opts: { name: string; email: string; subject: string; message: string; source?: string }) => {
    const { error } = await supabase.from('contact_messages').insert({
      name: opts.name,
      email: opts.email,
      subject: opts.subject,
      message: opts.message,
      source: opts.source || 'website',
    });
    if (error) throw new Error(error.message);
  };

  useEffect(() => {
    supabase.auth.getSession().then(({ data: { session } }) => {
      setSession(session);
      setAuthLoading(false);
      if (session?.user?.id) {
        supabase.from('billing_profiles').select('*').eq('owner_id', session.user.id).single()
          .then(({ data }) => setBillingProfile(data));
      }
    });
    const { data: { subscription } } = supabase.auth.onAuthStateChange((_event, session) => {
      setSession(session);
      if (session?.user?.id) {
        supabase.from('billing_profiles').select('*').eq('owner_id', session.user.id).single()
          .then(({ data }) => setBillingProfile(data));
      } else {
        setBillingProfile(null);
      }
    });
    return () => subscription.unsubscribe();
  }, []);

  useEffect(() => {
    const raw = localStorage.getItem('duckpond_cfg');
    if (raw) {
      try { setCfg(JSON.parse(raw)); } catch { }
    }
  }, []);
  useEffect(() => {
    localStorage.setItem('duckpond_cfg', JSON.stringify(cfg));
  }, [cfg]);

  const authHeaders = (): Record<string, string> => {
    const token = session?.access_token;
    if (!token) return { 'Content-Type': 'application/json' };
    return { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` };
  };

  const authHeadersRaw = (): Record<string, string> => {
    const token = session?.access_token;
    if (!token) return {};
    return { Authorization: `Bearer ${token}` };
  };

  const handleAuth = async () => {
    setAuthError('');
    if (authMode === 'signup') {
      try {
        const verifyRes = await fetch('https://api.truelist.io/v1/verify', {
          method: 'POST',
          headers: {
            'Authorization': 'Bearer eyJhbGciOiJIUzI1NiJ9.eyJpZCI6IjYzYTE5YjIyLTk5NzEtNDNiYi05NjBmLTBhMzE3YzYxOGY3ZSIsImV4cGlyZXNfYXQiOm51bGx9.Urnqa9fZBKl3e8zkW0S2QJs5pxl36GIW-tcHpDhKjqU',
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({ email: authEmail })
        });
        if (verifyRes.ok) {
          const verifyData = await verifyRes.json();
          if (verifyData?.result === 'invalid' || verifyData?.disposable === true) {
            setAuthError('Registration blocked: Please provide a valid, non-disposable email address.');
            return;
          }
        }
      } catch (err) {
        console.error('Email validation check failed:', err);
      }

      const { data, error } = await supabase.auth.signUp({ email: authEmail, password: authPassword });
      if (error) setAuthError(error.message);
      else if (!data?.session) setAuthError('Check your email for the confirmation link.');
    } else {
      const { error } = await supabase.auth.signInWithPassword({ email: authEmail, password: authPassword });
      if (error) setAuthError(error.message);
    }
  };

  const handleLogout = async () => {
    await supabase.auth.signOut();
    setSession(null);
  };

  useEffect(() => {
    if (cfg.coord) checkStatus();
  }, [cfg.coord]);

  const checkStatus = async () => {
    if (!cfg.coord) { setStatusOk(false); return; }
    try {
      const res = await fetch(`${cfg.coord}/status`);
      if (!res.ok) throw new Error(res.statusText);
      const data = await res.json();
      setStatusOk(true);
      addLog(`status OK — ${data.parquet_files || 'synthetic'} files in ${cfg.bucket}`, 'ok');
    } catch (e: any) {
      setStatusOk(false);
      addLog(`status check failed: ${e.message}`, 'err');
    }
  };

  const runQuery = async () => {
    if (!cfg.coord) { addLog('Set coordinator URL first', 'err'); return; }
    if (!querySql) return;
    setQueryRunning(true);
    setLastExecutedSql(querySql);
    setQueryStatus('');
    setResults(null);
    addLog(`query dispatched to ${cfg.workers} workers...`, 'info');

    const ctrl = new AbortController();
    abortCtrl.current = ctrl;

    try {
      const res = await fetch(`${cfg.coord}/query`, {
        method: 'POST',
        headers: authHeaders(),
        body: JSON.stringify({ sql: querySql, limit: 1000, output_mode: 'flight_stream', compute_tier: computeTier, secrets: cfg.secrets || [] }),
        signal: ctrl.signal
      });
      if (!res.ok) throw new Error(await res.text());

      const reader = res.body?.getReader();
      const decoder = new TextDecoder("utf-8");

      let allColumns: string[] = [];
      let allRows: any[] = [];
      let totalRows = 0;

      if (reader) {
        let buffer = "";

        const processLine = (line: string) => {
          if (!line.trim()) return false;
          let msg;
          try {
            msg = JSON.parse(line);
          } catch (jsonErr) {
            return false; // Ignore partial lines
          }

          // Handle DML/DDL error responses: {error: "..."} format
          if (msg.error && !msg.type) {
            setQueryRunning(false);
            addLog(`query error: ${msg.error}`, 'err');
            setQueryStatus(`error: ${msg.error}`);
            return true;
          }

          if (msg.status === 'ok' && !msg.type) {
            setQueryStatus(`done! ${msg.msg || ''}`);
            addLog(msg.msg || 'query complete', 'ok');
            if (msg.data && msg.data.length > 0) {
              setResults({ columns: Object.keys(msg.data[0] || {}), rows: msg.data, row_count: msg.data.length });
            }
            // Auto-refresh catalog after DDL/DML operations
            const msgLower = (msg.msg || '').toLowerCase();
            if (msgLower.includes('created') || msgLower.includes('dropped') || msgLower.includes('altered') || msgLower.includes('mutated') || msgLower.includes('updated') || msgLower.includes('deleted')) {
              loadSchema();
            }
            return true; // Stop processing
          }

          if (msg.type === 'error') {
            setQueryRunning(false);
            addLog(`query error: ${msg.msg}`, 'err');
            setQueryStatus('error');
            return true;
          } else if (msg.type === 'info') {
            if (msg.msg.includes('Provisioning')) setQueryStatus('🚀 Provisioning Hardware Layer...');
            else if (msg.msg.includes('Booted')) setQueryStatus('🔥 Booting MicroVMs...');
            else if (msg.msg.includes('scattered')) setQueryStatus('⚡ Executing Map/Reduce...');
            else setQueryStatus('⏳ ' + msg.msg);
          } else if (msg.type === 'data') {
            if (allColumns.length === 0) allColumns = msg.columns;
            allRows = allRows.concat(msg.rows);
            totalRows += msg.row_count;
            setResults({ columns: allColumns, rows: allRows, row_count: totalRows });
            setQueryStatus(`streaming... ${totalRows} rows`);
          } else if (msg.type === "done") {
            setQueryStatus(`done! ${msg.total_rows} rows streamed`);
            addLog(`query complete — ${msg.total_rows} rows streamed successfully`, 'ok');
          }
          return false;
        };

        while (true) {
          const { done, value } = await reader.read();

          if (value) {
            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n');
            buffer = lines.pop() || ""; // keep incomplete JSON line in buffer

            let shouldBreak = false;
            for (const line of lines) {
              if (processLine(line)) {
                shouldBreak = true;
                break;
              }
            }
            if (shouldBreak) break;
          }

          if (done) {
            if (buffer.trim()) processLine(buffer.trim());
            break;
          }
        }
      }
    } catch (e: any) {
      if (e.name === 'AbortError') {
        addLog('Socket stream severed automatically.', 'info');
      } else {
        addLog(`query error: ${e.message}`, 'err');
        setQueryStatus('error — see logs');
      }
    } finally {
      abortCtrl.current = null;
      setQueryRunning(false);
    }
  };

  // ---- CSV / JSON Export ---------------------------------------------------
  const exportResults = async (format: string) => {
    if (!results || results.rows.length === 0) return;
    
    if (format === 'parquet') {
      if (!lastExecutedSql) {
        addLog('No query history found for Parquet export.', 'err');
        return;
      }
      addLog('Requesting binary Parquet generation from backend...', 'info');
      setExporting(true);
      try {
        const headers = await (async () => {
          const { data } = await supabase.auth.getSession();
          const token = data.session?.access_token;
          if (!token) return {};
          return { 'Authorization': `Bearer ${token}` };
        })();

        const response = await fetch(cfg.coord + '/export/parquet', {
          method: 'POST',
          headers: { ...headers, 'Content-Type': 'application/json' } as HeadersInit,
          body: JSON.stringify({ sql: lastExecutedSql, compute_tier: computeTier })
        });

        if (!response.ok) {
          const err = await response.text();
          throw new Error(err || 'Failed to generate Parquet file');
        }

        const blob = await response.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `shikipond_export_${Date.now()}.parquet`;
        a.click();
        URL.revokeObjectURL(url);
        addLog(`Parquet export complete! Downloaded successfully.`, 'ok');
      } catch (err: any) {
        addLog(`Parquet export failed: ${err.message}`, 'err');
      } finally {
        setExporting(false);
      }
      return;
    }

    let content: string;
    let mime: string;
    let ext: string;

    if (format === 'csv') {
      const header = results.columns.join(',');
      const rows = results.rows.map(r => results.columns.map(c => {
        const v = String(r[c] ?? '');
        return v.includes(',') || v.includes('"') || v.includes('\n') ? `"${v.replace(/"/g, '""')}"` : v;
      }).join(','));
      content = [header, ...rows].join('\n');
      mime = 'text/csv';
      ext = 'csv';
    } else {
      content = JSON.stringify(results.rows, null, 2);
      mime = 'application/json';
      ext = 'json';
    }

    const blob = new Blob([content], { type: mime });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `shikipond_results_${Date.now()}.${ext}`;
    a.click();
    URL.revokeObjectURL(url);
    addLog(`Exported ${results.rows.length} rows as ${ext.toUpperCase()}`, 'ok');
  };

  // ---- SQL Autocomplete ---------------------------------------------------
  const getSuggestions = (sql: string, cursorPos: number): string[] => {
    const before = sql.slice(0, cursorPos);
    const wordMatch = before.match(/[\w.]*$/);
    const prefix = (wordMatch?.[0] || '').toLowerCase();
    if (prefix.length < 2) return [];

    const keywords = ['SELECT', 'FROM', 'WHERE', 'INSERT INTO', 'UPDATE', 'DELETE FROM', 'CREATE TABLE', 'DROP TABLE', 'ALTER TABLE', 'ADD COLUMN', 'GROUP BY', 'ORDER BY', 'LIMIT', 'JOIN', 'LEFT JOIN', 'INNER JOIN', 'ON', 'AS', 'AND', 'OR', 'NOT', 'IN', 'BETWEEN', 'LIKE', 'IS NULL', 'IS NOT NULL', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'DISTINCT', 'HAVING', 'UNION', 'VALUES', 'SET'];
    const tableNames = schemaTables.map(t => t.name);
    const colNames = schemaTables.flatMap(t => (t.columns || []).map((c: any) => c.name));
    const all = [...new Set([...keywords, ...tableNames, ...colNames])];

    return all.filter(s => s.toLowerCase().startsWith(prefix)).slice(0, 8);
  };

  const handleEditorKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (!showSuggestions || suggestions.length === 0) {
      if (e.key === 'Tab') {
        e.preventDefault();
        const el = e.currentTarget;
        const start = el.selectionStart;
        const end = el.selectionEnd;
        setQuerySql(querySql.substring(0, start) + '  ' + querySql.substring(end));
        setTimeout(() => { el.selectionStart = el.selectionEnd = start + 2; }, 0);
      }
      return;
    }

    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setSuggestionIdx(i => Math.min(i + 1, suggestions.length - 1));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setSuggestionIdx(i => Math.max(i - 1, 0));
    } else if (e.key === 'Tab' || e.key === 'Enter') {
      if (suggestions[suggestionIdx]) {
        e.preventDefault();
        applySuggestion(suggestions[suggestionIdx]);
      }
    } else if (e.key === 'Escape') {
      setShowSuggestions(false);
    }
  };

  const applySuggestion = (word: string) => {
    const el = editorRef.current;
    if (!el) return;
    const pos = el.selectionStart;
    const before = querySql.slice(0, pos);
    const after = querySql.slice(pos);
    const wordMatch = before.match(/[\w.]*$/);
    const prefixLen = wordMatch?.[0]?.length || 0;
    const newSql = before.slice(0, before.length - prefixLen) + word + ' ' + after;
    setQuerySql(newSql);
    setShowSuggestions(false);
    const newPos = before.length - prefixLen + word.length + 1;
    setTimeout(() => { el.selectionStart = el.selectionEnd = newPos; el.focus(); }, 0);
  };

  const handleEditorInput = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    const val = e.target.value;
    setQuerySql(val);
    const pos = e.target.selectionStart;
    const s = getSuggestions(val, pos);
    setSuggestions(s);
    setSuggestionIdx(0);
    setShowSuggestions(s.length > 0);

    // Auto-resize logic
    if (editorRef.current) {
      editorRef.current.style.height = '160px'; // Reset to base height to calculate scrollHeight
      const newHeight = editorRef.current.scrollHeight;

      // Calculate viewport bottom constraint
      const rect = editorRef.current.getBoundingClientRect();
      const maxAvailableHeight = window.innerHeight - rect.top - 80; // 80px margin from bottom

      const finalHeight = Math.min(newHeight, Math.max(160, maxAvailableHeight));
      editorRef.current.style.height = finalHeight + 'px';
      editorRef.current.style.overflowY = newHeight > finalHeight ? 'auto' : 'hidden';
    }
  };

  const loadSchema = async () => {
    if (!cfg.coord) return;
    try {
      const res = await fetch(`${cfg.coord}/schema`, { headers: authHeadersRaw() });
      const data = await res.json();
      setSchemaTables(data.tables || []);
      if (data.namespace) setSchemaNamespace(data.namespace);
    } catch (e: any) {
      addLog(`schema error: ${e.message}`, 'err');
    }
  };

  const dropTable = async (name: string) => {
    if (!cfg.coord) return;
    if (!confirm(`Delete table "${name}"? This removes all data from S3 and the catalog.`)) return;
    try {
      await fetch(`${cfg.coord}/v1/namespaces/${schemaNamespace}/tables/${name}`, { method: 'DELETE', headers: authHeadersRaw() });
      addLog(`dropped table: ${name}`, 'ok');
      loadSchema();
    } catch (e: any) {
      addLog(`drop error: ${e.message}`, 'err');
    }
  };

  const startIngest = async () => {
    if (!cfg.coord) return;
    if (!ingestForm.uri || !ingestForm.table) return;
    setIngestStatus('ingesting...');
    addLog(`ingest started: ${ingestForm.uri} → ${ingestForm.table}`, 'info');
    try {
      const res = await fetch(`${cfg.coord}/ingest`, {
        method: 'POST',
        headers: authHeaders(),
        body: JSON.stringify({ source_s3_uri: ingestForm.uri, table_name: ingestForm.table, compute_tier: ingestForm.tier })
      });
      const data = await res.json();
      setIngestStatus(`✓ ${data.status}`);
      addLog(`ingest accepted for lh.${ingestForm.table}`, 'ok');
    } catch (e: any) {
      setIngestStatus(`Error: ${e.message}`);
      addLog(`ingest error: ${e.message}`, 'err');
    }
  };

  const handleFileUpload = async (file: File) => {
    if (!cfg.coord) { addLog('Set coordinator URL first', 'err'); return; }
    const tableName = uploadTable || file.name.replace(/\.[^.]+$/, '').replace(/[^a-zA-Z0-9_]/g, '_');
    if (!uploadTable) setUploadTable(tableName);

    setUploadStatus(`Uploading ${file.name} (${(file.size / 1024 / 1024).toFixed(1)} MB)...`);
    addLog(`upload started: ${file.name} → ${tableName}`, 'info');

    try {
      const formData = new FormData();
      formData.append('file', file);
      formData.append('table_name', tableName);

      const res = await fetch(`${cfg.coord}/upload`, {
        method: 'POST',
        headers: authHeadersRaw(),
        body: formData
      });
      const data = await res.json();
      if (data.error) {
        setUploadStatus(`✗ ${data.error}`);
        addLog(`upload error: ${data.error}`, 'err');
      } else {
        setUploadStatus(`✓ ${data.status}`);
        addLog(`upload complete: ${tableName}`, 'ok');
      }
    } catch (e: any) {
      setUploadStatus(`Error: ${e.message}`);
      addLog(`upload error: ${e.message}`, 'err');
    }
  };

  // Org API functions
  const loadOrgs = async () => {
    if (!cfg.coord || !session) return;
    try {
      const res = await fetch(`${cfg.coord}/orgs`, { headers: authHeadersRaw() });
      if (!res.ok) {
        const errText = await res.text();
        addLog(`org fetch failed (${res.status}): ${errText}`, 'err');
        return;
      }
      const data = await res.json();
      setOrgs(data.orgs || []);
      if (data.orgs?.length > 0) {
        addLog(`org loaded: ${data.orgs[0].name} (${data.orgs[0].role})`, 'info');
        loadOrgMembers(data.orgs[0].slug);
      }
    } catch (e: any) { addLog(`org error: ${e.message}`, 'err'); }
  };

  const loadOrgMembers = async (slug: string) => {
    if (!cfg.coord) return;
    try {
      const res = await fetch(`${cfg.coord}/orgs/${slug}/members`, { headers: authHeadersRaw() });
      const data = await res.json();
      setOrgMembers(data.members || []);
    } catch { setOrgMembers([]); }
  };

  const createOrg = async () => {
    if (!cfg.coord || !newOrgName || !newOrgSlug) return;
    setOrgStatus('');
    try {
      const res = await fetch(`${cfg.coord}/orgs`, {
        method: 'POST', headers: authHeaders(),
        body: JSON.stringify({ name: newOrgName, slug: newOrgSlug })
      });
      const data = await res.json();
      if (data.detail) { setOrgStatus(`Error: ${data.detail}`); return; }
      setOrgStatus(`✓ Created ${data.name} (${data.namespace})`);
      setNewOrgName(''); setNewOrgSlug('');
      addLog(`org created: ${data.name}`, 'ok');
      loadOrgs();
    } catch (e: any) { setOrgStatus(`Error: ${e.message}`); }
  };

  const inviteMember = async (slug: string) => {
    if (!cfg.coord || !inviteEmail) return;
    try {
      const res = await fetch(`${cfg.coord}/orgs/${slug}/members`, {
        method: 'POST', headers: authHeaders(),
        body: JSON.stringify({ email: inviteEmail })
      });
      const data = await res.json();
      if (data.detail) { setOrgStatus(`Error: ${data.detail}`); return; }
      setOrgStatus(`✓ ${data.status}`);
      setInviteEmail('');
      addLog(`invited ${inviteEmail} to ${slug}`, 'ok');
      loadOrgMembers(slug);
    } catch (e: any) { setOrgStatus(`Error: ${e.message}`); }
  };

  const removeMember = async (slug: string, email: string) => {
    if (!confirm(`Remove ${email} from ${slug}?`)) return;
    try {
      await fetch(`${cfg.coord}/orgs/${slug}/members/${encodeURIComponent(email)}`, { method: 'DELETE', headers: authHeadersRaw() });
      addLog(`removed ${email} from ${slug}`, 'ok');
      loadOrgMembers(slug);
    } catch (e: any) { addLog(`remove error: ${e.message}`, 'err'); }
  };

  const deleteOrg = async (slug: string, name: string) => {
    if (!confirm(`DELETE "${name}" and ALL its data? This cannot be undone.`)) return;
    if (!confirm(`Are you absolutely sure? All tables, uploads, and member associations will be permanently deleted.`)) return;
    try {
      const res = await fetch(`${cfg.coord}/orgs/${slug}`, { method: 'DELETE', headers: authHeadersRaw() });
      const data = await res.json();
      if (data.detail) { addLog(`delete org error: ${data.detail}`, 'err'); return; }
      addLog(`org deleted: ${name}`, 'ok');
      setOrgs([]);
      setOrgMembers([]);
    } catch (e: any) { addLog(`delete org error: ${e.message}`, 'err'); }
  };

  const deleteAccount = async () => {
    if (!confirm('DELETE your account and all your data? This cannot be undone.')) return;
    if (!confirm('Last chance. Your tables, uploads, and org memberships will be permanently deleted.')) return;
    try {
      const res = await fetch(`${cfg.coord}/account`, { method: 'DELETE', headers: authHeadersRaw() });
      const data = await res.json();
      if (!res.ok || data.detail) {
        const msg = data.detail || `Server error ${res.status}`;
        addLog(`delete account error: ${msg}`, 'err');
        alert(`Could not delete account: ${msg}`);
        return;
      }
      addLog('account deleted', 'ok');
    } catch (e: any) {
      // Coordinator unreachable — still sign out locally
      addLog(`coordinator unreachable during delete: ${e.message}`, 'err');
      alert(`Warning: could not reach coordinator (${e.message}). Signing you out locally — please delete your account from the Supabase dashboard if needed.`);
    }
    await supabase.auth.signOut();
    setSession(null);
  };

  // dbt API functions
  const loadDbtJobs = async () => {
    if (!cfg.coord) return;
    try {
      const res = await fetch(`${cfg.coord}/dbt/jobs`, { headers: authHeadersRaw() });
      const data = await res.json();
      setDbtJobs(data.jobs || []);
    } catch { /* ignore */ }
  };

  const runDbt = async () => {
    if (!cfg.coord) return;
    setDbtRunning(true); setDbtLogs([]); setDbtTables([]); setDbtStatus('queued');
    addLog(`dbt ${dbtCommand} starting...`, 'info');
    try {
      const body: any = { command: dbtCommand };
      if (dbtSource === 'git' && dbtGitUrl) body.git_url = dbtGitUrl;
      const res = await fetch(`${cfg.coord}/dbt/run`, {
        method: 'POST', headers: authHeaders(), body: JSON.stringify(body)
      });
      if (!res.ok) {
        const errText = await res.text();
        setDbtStatus('error'); setDbtLogs([`API error ${res.status}: ${errText}`]); setDbtRunning(false);
        addLog(`dbt API error: ${res.status}`, 'err');
        return;
      }
      const data = await res.json();
      if (data.error) { setDbtStatus('error'); setDbtLogs([data.error]); setDbtRunning(false); addLog(`dbt error: ${data.error}`, 'err'); return; }
      if (!data.job_id) { setDbtStatus('error'); setDbtLogs(['No job ID returned']); setDbtRunning(false); return; }
      setDbtJobId(data.job_id);
      addLog(`dbt job ${data.job_id} queued`, 'info');
      pollDbtJob(data.job_id);
    } catch (e: any) { setDbtStatus('error'); setDbtRunning(false); addLog(`dbt error: ${e.message}`, 'err'); }
  };

  const pollDbtJob = async (jobId: string) => {
    let retries = 0;
    const maxRetries = 200; // ~5 min at 1.5s intervals
    const poll = async () => {
      if (retries++ > maxRetries) {
        setDbtRunning(false); setDbtStatus('timeout');
        addLog('dbt poll timed out', 'err');
        return;
      }
      try {
        const res = await fetch(`${cfg.coord}/dbt/logs/${jobId}`, { headers: authHeadersRaw() });
        if (!res.ok) { setTimeout(poll, 2000); return; }
        const data = await res.json();
        const status = data.status || 'running';
        setDbtStatus(status);
        setDbtLogs(data.logs || []);
        setDbtTables(data.tables || []);

        const terminal = ['success', 'failed', 'error', 'timeout'];
        if (terminal.includes(status)) {
          setDbtRunning(false);
          addLog(`dbt ${status}${data.tables?.length ? ` — ${data.tables.length} tables` : ''}`, status === 'success' ? 'ok' : 'err');
          loadDbtJobs();
          loadSchema();
        } else {
          setTimeout(poll, 1500);
        }
      } catch { setTimeout(poll, 2000); }
    };
    setTimeout(poll, 2000);
  };

  const uploadDbtProject = async (file: File) => {
    if (!cfg.coord) return;
    setDbtRunning(true); setDbtLogs([]); setDbtTables([]); setDbtStatus('uploading');
    addLog('uploading dbt project...', 'info');
    const formData = new FormData();
    formData.append('file', file);
    formData.append('command', dbtCommand);
    try {
      const res = await fetch(`${cfg.coord}/dbt/upload`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${session?.access_token || ''}` },
        body: formData
      });
      const data = await res.json();
      if (data.error) { setDbtStatus('error'); setDbtLogs([data.error]); setDbtRunning(false); return; }
      setDbtJobId(data.job_id);
      pollDbtJob(data.job_id);
    } catch (e: any) { setDbtStatus('error'); setDbtRunning(false); }
  };

  // Load orgs when session changes
  useEffect(() => {
    if (!cfg.coord || !session?.access_token) return;
    let cancelled = false;
    const token = session.access_token;
    fetch(`${cfg.coord}/orgs`, { headers: { Authorization: `Bearer ${token}` } })
      .then(res => res.ok ? res.json() : null)
      .then(data => {
        if (cancelled || !data) return;
        const orgList = data.orgs || [];
        setOrgs(orgList);
        if (orgList.length > 0) loadOrgMembers(orgList[0].slug);
      })
      .catch(() => { });
    return () => { cancelled = true; };
  }, [session, cfg.coord]);

  if (authLoading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100vh', background: 'var(--bg-core)' }}>
        <div style={{ textAlign: 'center' }}>
          <h1 style={{ fontFamily: 'var(--font-display)', marginBottom: '8px' }}>shikipond</h1>
          <p className="text-muted">Loading...</p>
        </div>
      </div>
    );
  }

  if (!session) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100vh', background: 'radial-gradient(circle at top right, var(--bg-gradient-2), var(--bg-core) 60%)', backgroundAttachment: 'fixed', position: 'relative', overflow: 'hidden' }}>

        {/* Ambient Login Water Animation with Hokkaido Trees */}
        <div className="pond-bg">
          <div className="particle-layer">
            {particles.map((p, i) => (
              <div key={i} className="particle" style={{ left: `${p.left}%`, animationDelay: `-${p.delay}s`, animationDuration: `${p.duration}s`, transform: `scale(${p.size})` }} />
            ))}
          </div>

          <div className="pond-wave pond-wave-1" />

          <div className="pond-tree t1">
            <div className="branch b1"><div className="foliage" /></div>
            <div className="branch b2"><div className="foliage" /></div>
            <div className="branch b3"><div className="foliage" /></div>
            <div className="branch b4"><div className="foliage" /></div>
          </div>
          <div className="pond-tree t2">
            <div className="branch b1"><div className="foliage" /></div>
            <div className="branch b2"><div className="foliage" /></div>
            <div className="branch b3"><div className="foliage" /></div>
            <div className="branch b4"><div className="foliage" /></div>
          </div>
          <div className="pond-tree t3">
            <div className="branch b1"><div className="foliage" /></div>
            <div className="branch b2"><div className="foliage" /></div>
            <div className="branch b3"><div className="foliage" /></div>
            <div className="branch b4"><div className="foliage" /></div>
          </div>

          <div className="pond-wave pond-wave-2" />

          <div className="pond-tree t4">
            <div className="branch b1"><div className="foliage" /></div>
            <div className="branch b2"><div className="foliage" /></div>
            <div className="branch b3"><div className="foliage" /></div>
            <div className="branch b4"><div className="foliage" /></div>
          </div>
          <div className="pond-tree t5">
            <div className="branch b1"><div className="foliage" /></div>
            <div className="branch b2"><div className="foliage" /></div>
            <div className="branch b3"><div className="foliage" /></div>
            <div className="branch b4"><div className="foliage" /></div>
          </div>

          <div className="pond-wave pond-wave-3" />

          <div className="pond-tree t6">
            <div className="branch b1"><div className="foliage" /></div>
            <div className="branch b2"><div className="foliage" /></div>
            <div className="branch b3"><div className="foliage" /></div>
            <div className="branch b4"><div className="foliage" /></div>
          </div>
          <div className="pond-tree t7">
            <div className="branch b1"><div className="foliage" /></div>
            <div className="branch b2"><div className="foliage" /></div>
            <div className="branch b3"><div className="foliage" /></div>
            <div className="branch b4"><div className="foliage" /></div>
          </div>
        </div>

        {/* Global Application Header */}
        <div style={{ position: 'absolute', top: '32px', left: '40px', right: '40px', zIndex: 20, display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '24px' }}>
            <button onClick={() => { setShowLanding(true); setShowAboutUs(false); }} style={{ background: 'none', border: 'none', padding: 0, margin: 0, cursor: 'pointer', fontFamily: 'var(--font-display)', fontSize: '30px', fontWeight: 'bold', color: '#fff', outline: 'none' }}>shikipond</button>
            {(!showLanding && !showAboutUs) && (
              <button onClick={() => setShowAboutUs(true)} style={{ background: 'none', border: 'none', color: 'var(--text-muted)', fontSize: '16px', fontWeight: '500', cursor: 'pointer', transition: 'color 0.2s', padding: 0 }} onMouseOver={e => e.currentTarget.style.color = '#fff'} onMouseOut={e => e.currentTarget.style.color = 'var(--text-muted)'}>About Us</button>
            )}
            {showAboutUs && (
              <button onClick={() => setShowAboutUs(false)} style={{ background: 'none', border: 'none', color: 'var(--text-muted)', fontSize: '16px', fontWeight: '500', cursor: 'pointer', transition: 'color 0.2s', padding: 0 }} onMouseOver={e => e.currentTarget.style.color = '#fff'} onMouseOut={e => e.currentTarget.style.color = 'var(--text-muted)'}>← Back</button>
            )}
          </div>

          {showLanding && !session && (
            <button 
              onClick={() => setShowLanding(false)} 
              style={{ background: 'none', border: 'none', color: 'var(--text-muted)', fontSize: '16px', fontWeight: '500', cursor: 'pointer', transition: 'color 0.2s', padding: 0 }} 
              onMouseOver={e => e.currentTarget.style.color = '#fff'} 
              onMouseOut={e => e.currentTarget.style.color = 'var(--text-muted)'}
            >
              Login
            </button>
          )}
        </div>

        {showAboutUs ? (
          <div style={{ position: 'absolute', top: 0, left: 0, right: 0, bottom: 0, zIndex: 10, display: 'flex', flexDirection: 'row', alignItems: 'flex-start', justifyContent: 'flex-start', padding: '120px 40px 40px 40px', gap: '80px', flexWrap: 'wrap' }}>

            <div style={{ maxWidth: '480px', textAlign: 'justify', marginBottom: '0', display: 'flex', flexDirection: 'column', gap: '16px' }}>
              <h2 style={{ fontFamily: 'var(--font-display)', fontSize: '32px', color: '#fff', margin: '0', textAlign: 'left' }}>About</h2>
              <p style={{ fontSize: '16px', color: 'var(--text-main)', lineHeight: '1.6', margin: 0 }}>
                shikipond's mission is to democratise access to the elastic lakehouse modern data stack.
              </p>
              <p style={{ fontSize: '16px', color: 'var(--text-main)', lineHeight: '1.6', margin: 0 }}>
                It was created for small, medium to large teams, and to be used with the <a href="https://ruddy.pro" target="_blank" rel="noopener noreferrer" style={{ color: 'var(--accent-primary)', textDecoration: 'none' }}>Ruddy IDE</a> (in case you want to).
              </p>
            </div>

            <div className="hero-feature-card" style={{ flex: 'none', width: '100%', maxWidth: '400px', padding: '32px', position: 'relative', overflow: 'hidden', textAlign: 'left', display: 'flex', flexDirection: 'column', gap: '16px' }}>
              <h3 style={{ fontFamily: 'var(--font-display)', fontSize: '20px', color: '#fff', margin: 0 }}>Contact Us</h3>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '12px', width: '100%' }}>
                <input type="text" placeholder="Name" value={aboutForm.name} onChange={e => setAboutForm(f => ({ ...f, name: e.target.value }))} style={{ width: '100%', padding: '10px', borderRadius: '6px', border: '1px solid var(--surface-border)', background: 'var(--surface-glass)', color: 'var(--text-main)', fontSize: '14px', outline: 'none', boxSizing: 'border-box' }} />
                <input type="email" placeholder="Email" value={aboutForm.email} onChange={e => setAboutForm(f => ({ ...f, email: e.target.value }))} style={{ width: '100%', padding: '10px', borderRadius: '6px', border: '1px solid var(--surface-border)', background: 'var(--surface-glass)', color: 'var(--text-main)', fontSize: '14px', outline: 'none', boxSizing: 'border-box' }} />
                <textarea placeholder="Message" rows={3} value={aboutForm.message} onChange={e => setAboutForm(f => ({ ...f, message: e.target.value }))} style={{ width: '100%', padding: '10px', borderRadius: '6px', border: '1px solid var(--surface-border)', background: 'var(--surface-glass)', color: 'var(--text-main)', fontSize: '14px', outline: 'none', boxSizing: 'border-box', resize: 'none' }} />
                {aboutStatus && <span style={{ fontSize: '13px', color: aboutStatus.startsWith('✓') ? 'var(--accent-success)' : 'var(--accent-danger)' }}>{aboutStatus}</span>}
                <button className="btn-primary" style={{ padding: '12px', justifyContent: 'center' }} onClick={async () => {
                  if (!aboutForm.name || !aboutForm.email || !aboutForm.message) { setAboutStatus('Please fill in all fields.'); return; }
                  try {
                    setAboutStatus('Sending...');
                    await sendContactEmail({ name: aboutForm.name, email: aboutForm.email, subject: 'Contact (About Us)', message: aboutForm.message });
                    setAboutStatus('✓ Message sent!');
                    setAboutForm({ name: '', email: '', message: '' });
                    setTimeout(() => setShowAboutUs(false), 1500);
                  } catch (e: any) { setAboutStatus(`Error: ${e.message}`); }
                }}>Send Message →</button>
              </div>
            </div>

          </div>
        ) : showLanding ? (
          <div style={{ position: 'absolute', top: 0, left: 0, right: 0, bottom: 0, zIndex: 10, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', textAlign: 'center', padding: '100px 40px 40px 40px' }}>
            <div style={{ display: 'flex', width: '100%', maxWidth: '1200px', alignItems: 'flex-start', justifyContent: 'space-between', gap: 'clamp(20px, 5vw, 80px)', padding: '0 40px', textAlign: 'left', minHeight: 0, transform: 'translateY(-30px)' }}>


              {/* Left Column: Core Branding */}
              <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-start', flex: 1 }}>
                <div style={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'flex-start', gap: '16px', marginBottom: '32px' }}>
                  <div style={{ padding: '8px 20px', borderRadius: '9999px', background: 'color-mix(in srgb, var(--accent-primary) 10%, transparent)', border: '1px solid color-mix(in srgb, var(--accent-primary) 30%, transparent)', fontSize: '14px', fontWeight: '500', color: 'var(--accent-primary)' }}>50 GB | STARTER <span style={{ opacity: 0.7, marginLeft: '8px' }}>$7.99</span></div>
                  <div style={{ padding: '8px 20px', borderRadius: '9999px', background: 'color-mix(in srgb, var(--accent-primary) 10%, transparent)', border: '1px solid color-mix(in srgb, var(--accent-primary) 30%, transparent)', fontSize: '14px', fontWeight: '500', color: 'var(--accent-primary)' }}>100 GB | PRO <span style={{ opacity: 0.7, marginLeft: '8px' }}>$14.99</span></div>
                  <div style={{ padding: '8px 20px', borderRadius: '9999px', background: 'color-mix(in srgb, var(--accent-primary) 10%, transparent)', border: '1px solid color-mix(in srgb, var(--accent-primary) 30%, transparent)', fontSize: '14px', fontWeight: '500', color: 'var(--accent-primary)' }}>1 TB | TEAM <span style={{ opacity: 0.7, marginLeft: '8px' }}>$99.00/10 seats</span></div>
                  <div style={{ padding: '8px 20px', borderRadius: '9999px', background: 'color-mix(in srgb, var(--accent-primary) 10%, transparent)', border: '1px solid color-mix(in srgb, var(--accent-primary) 30%, transparent)', fontSize: '14px', fontWeight: '500', color: 'var(--accent-primary)' }}>TPC-H Verified</div>
                </div>

                <h1 style={{ fontFamily: 'var(--font-display)', fontSize: 'clamp(32px, 7vh, 64px)', marginBottom: 'min(24px, 3vh)', lineHeight: '1.1', textAlign: 'left', letterSpacing: '-0.02em', color: '#fff' }}>
                  Serverless <br />
                  Lakehouse <br />
                  <span style={{ color: 'var(--accent-primary)' }}>Engine</span>
                </h1>

                <button
                  className="btn-primary"
                  style={{ fontSize: '18px', padding: '16px 40px' }}
                  onClick={() => setShowLanding(false)}>
                  Start 7-day trial →
                </button>
              </div>

              {/* Right Column: Features Card with Animation */}
              <div style={{ display: 'flex', flexDirection: 'column', flex: 1, maxWidth: '500px' }}>
                <div className="hero-feature-card" style={{ width: '100%', height: '480px', padding: '60px 40px', position: 'relative', overflow: 'hidden', marginBottom: '20px' }}>
                  <ul key={featureIndex} style={{ listStyle: 'none', padding: 0, margin: 0, display: 'flex', flexDirection: 'column', gap: '36px', animation: 'featureFade 0.6s ease-out' }}>
                    {featureSets[featureIndex].map((feature, i) => (
                      <li key={i} style={{ display: 'flex', gap: '16px', alignItems: 'flex-start' }}>
                        <span style={{ color: 'var(--accent-primary)', fontSize: '30px', lineHeight: '1' }}>•</span>
                        <p style={{ margin: 0, fontSize: '19px', opacity: 0.9, lineHeight: '1.5' }}>{feature}</p>
                      </li>
                    ))}
                  </ul>
                  {/* Progress Indicator */}
                  <div style={{ position: 'absolute', bottom: 0, left: 0, height: '4px', background: 'var(--accent-primary)', width: '100%', transformOrigin: 'left', animation: 'featureProgress 15s linear infinite' }} />
                </div>

                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', alignSelf: 'center', gap: '12px', color: 'var(--text-muted)', fontSize: '18px', fontWeight: '500' }}>
                  <span>Powered by</span>
                  <a href="https://iceberg.apache.org" target="_blank" rel="noopener noreferrer" style={{ display: 'flex', opacity: 0.9, transition: 'opacity 0.2s' }} onMouseOver={e => e.currentTarget.style.opacity = '1'} onMouseOut={e => e.currentTarget.style.opacity = '0.9'}>
                    <img src="https://cdn.jsdelivr.net/gh/homarr-labs/dashboard-icons/svg/apache-iceberg.svg" alt="Apache Iceberg" style={{ height: '32px' }} />
                  </a>
                  <a href="https://duckdb.org/" target="_blank" rel="noopener noreferrer" style={{ display: 'flex', color: 'inherit', opacity: 0.9, transition: 'opacity 0.2s' }} onMouseOver={e => e.currentTarget.style.opacity = '1'} onMouseOut={e => e.currentTarget.style.opacity = '0.9'}>
                    <svg role="img" viewBox="0 0 24 24" fill="currentColor" style={{ height: '32px' }} xmlns="http://www.w3.org/2000/svg"><title>DuckDB</title><path d="M12 0C5.363 0 0 5.363 0 12s5.363 12 12 12 12-5.363 12-12S18.637 0 12 0zM9.502 7.03a4.974 4.974 0 0 1 4.97 4.97 4.974 4.974 0 0 1-4.97 4.97A4.974 4.974 0 0 1 4.532 12a4.974 4.974 0 0 1 4.97-4.97zm6.563 3.183h2.351c.98 0 1.787.782 1.787 1.762s-.807 1.789-1.787 1.789h-2.351v-3.551z" /></svg>
                  </a>
                </div>
              </div>

            </div>
          </div>
        ) : (
          <div className="hero-feature-card" style={{ flex: 'none', padding: '40px', width: '400px', display: 'flex', flexDirection: 'column', gap: '20px', zIndex: 10, position: 'relative', marginTop: '-100px' }}>
            <div style={{ textAlign: 'center' }}>
              <h1 style={{ fontFamily: 'var(--font-display)', fontSize: '28px', marginBottom: '4px' }}>Welcome</h1>
              <p className="text-muted" style={{ fontSize: '14px' }}>Serverless Lakehouse Engine</p>
            </div>

            <div style={{ display: 'flex', gap: '0', borderRadius: '8px', overflow: 'hidden', border: '1px solid var(--surface-border)' }}>
              <button
                onClick={() => setAuthMode('login')}
                style={{ flex: 1, padding: '10px', border: 'none', cursor: 'pointer', fontFamily: 'var(--font-display)', fontWeight: 600, fontSize: '13px', background: authMode === 'login' ? 'var(--accent-primary)' : 'var(--surface-glass)', color: authMode === 'login' ? 'white' : 'var(--text-main)', transition: 'background 0.2s' }}
              >Sign In</button>
              <button
                onClick={() => setAuthMode('signup')}
                style={{ flex: 1, padding: '10px', border: 'none', cursor: 'pointer', fontFamily: 'var(--font-display)', fontWeight: 600, fontSize: '13px', background: authMode === 'signup' ? 'var(--accent-primary)' : 'var(--surface-glass)', color: authMode === 'signup' ? 'white' : 'var(--text-main)', transition: 'background 0.2s' }}
              >Sign Up</button>
            </div>

            <input
              type="email" placeholder="Email address" value={authEmail}
              onChange={e => setAuthEmail(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && handleAuth()}
              style={{ width: '100%', padding: '12px', borderRadius: '6px', border: '1px solid var(--surface-border)', background: 'var(--surface-glass)', color: 'var(--text-main)', fontSize: '14px', outline: 'none', boxSizing: 'border-box' }}
            />
            <input
              type="password" placeholder="Password (min 6 characters)" value={authPassword}
              onChange={e => setAuthPassword(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && handleAuth()}
              style={{ width: '100%', padding: '12px', borderRadius: '6px', border: '1px solid var(--surface-border)', background: 'var(--surface-glass)', color: 'var(--text-main)', fontSize: '14px', outline: 'none', boxSizing: 'border-box' }}
            />

            {authError && (
              <div style={{ padding: '10px', borderRadius: '6px', background: authError.includes('Check your email') ? 'rgba(16,185,129,0.15)' : 'rgba(239,68,68,0.15)', color: authError.includes('Check your email') ? 'var(--accent-success)' : 'var(--accent-danger)', fontSize: '13px' }}>
                {authError}
              </div>
            )}

            <button className="btn-primary" onClick={handleAuth} style={{ width: '100%', justifyContent: 'center', padding: '12px' }}>
              {authMode === 'login' ? 'Sign In →' : 'Create Account →'}
            </button>
            {authMode === 'login' && (
              <button onClick={async () => {
                if (!authEmail) { setAuthError('Enter your email first'); return; }
                const { error } = await supabase.auth.resetPasswordForEmail(authEmail);
                if (error) setAuthError(error.message);
                else setAuthError('Check your email for the password reset link.');
              }} style={{ background: 'none', border: 'none', color: 'var(--text-muted)', cursor: 'pointer', fontSize: '13px', textAlign: 'center', width: '100%' }}>
                Forgot Password?
              </button>
            )}
          </div>
        )}

      </div>
    );
  }

  return (
    <div className="app-layout">
      {/* Sidebar Navigation & Config */}
      <div className="sidebar">
        {/* Collapsed state — just the logo icon */}
        <div className="sidebar-collapsed-icon">
          <svg width="36" height="36" viewBox="0 0 36 36" xmlns="http://www.w3.org/2000/svg" style={{ color: 'var(--text-main)' }}>
            <ellipse cx="18" cy="20" rx="16" ry="8" fill="none" stroke="var(--water-3-mid)" strokeWidth="1" opacity="0.5" />
            <ellipse cx="18" cy="20" rx="11" ry="5" fill="none" stroke="var(--water-2-mid)" strokeWidth="1.5" opacity="0.8" />
            <ellipse cx="18" cy="20" rx="6" ry="3" fill="var(--water-1-mid)" opacity="0.9" />
            <text x="18" y="22" fontFamily="var(--font-display), Space Grotesk, sans-serif" fontSize="16" fontWeight="700" fill="currentColor" textAnchor="middle" letterSpacing="-0.5px">SP</text>
          </svg>
        </div>

        {/* Expanded content */}
        <div className="sidebar-content" style={{ padding: '24px', display: 'flex', flexDirection: 'column', gap: '24px', flex: 1, overflow: 'hidden' }}>
          <div>
            <h1 style={{ fontSize: '24px', margin: '0 0 4px 0' }}>shikipond</h1>
            <p className="text-muted" style={{ fontSize: '13px' }}>Serverless Lakehouse Engine</p>
          </div>

          {/* User Info & Billing */}
          <div className="glass-panel" style={{ padding: '14px 16px', display: 'flex', flexDirection: 'column', gap: '12px' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
              <div style={{ width: '32px', height: '32px', borderRadius: '50%', background: 'var(--accent-primary)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '14px', fontWeight: 600, flexShrink: 0 }}>
                {(session.user.email || '?')[0].toUpperCase()}
              </div>
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ fontSize: '13px', fontWeight: 500, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{session.user.email}</div>
                <div className="text-muted" style={{ fontSize: '11px', textTransform: 'capitalize' }}>
                  {billingProfile ? (`${billingProfile.plan_tier} Plan`) : (orgs.length > 0 ? orgs[0].name : 'Individual')}
                </div>
              </div>
              <button onClick={handleLogout} title="Sign Out" style={{ background: 'none', border: 'none', color: 'var(--text-muted)', cursor: 'pointer', padding: '4px', fontSize: '16px' }}>↩</button>
            </div>

            {billingProfile && (
              <div style={{ borderTop: '1px solid var(--surface-border)', paddingTop: '12px', display: 'flex', flexDirection: 'column', gap: '8px' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '11px' }}>
                  <span className="text-muted">Compute Balance</span>
                  <span style={{ fontWeight: 600, color: billingProfile.compute_credit_balance <= 0 ? 'var(--accent-danger)' : 'var(--accent-success)' }}>
                    ${parseFloat(billingProfile.compute_credit_balance).toFixed(2)}
                  </span>
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '11px' }}>
                    <span className="text-muted">Storage Quota</span>
                    <span>{(billingProfile.storage_limit_bytes / (1024 ** 3)).toFixed(1)} GB</span>
                  </div>
                  <div style={{ width: '100%', height: '4px', background: 'var(--surface-border)', borderRadius: '2px', overflow: 'hidden' }}>
                    {/* Simulated usage strictly for UI visibility, actual limit checked on backend */}
                    <div style={{ width: '2%', height: '100%', background: 'var(--water-1-mid)' }} />
                  </div>
                </div>
                {billingProfile.trial_ends_at && new Date(billingProfile.trial_ends_at) > new Date() && (
                  <div style={{ background: 'var(--accent-primary)', color: 'white', fontSize: '10px', padding: '4px 6px', borderRadius: '4px', textAlign: 'center', marginTop: '4px', fontWeight: 600 }}>
                    7-Day Trial Active
                  </div>
                )}
                <button className="btn-secondary" onClick={() => setShowUpgradeModal(true)} style={{ marginTop: '8px', padding: '6px', fontSize: '11px', justifyContent: 'center' }}>
                  Upgrade Plan
                </button>
              </div>
            )}
          </div>

          {/* Theme & Season Toggles */}
          <div style={{ display: 'flex', gap: '12px', justifyContent: 'center', fontSize: '12px' }}>
            <button
              onClick={() => setThemeMode(themeMode === 'light' ? 'dark' : 'light')}
              className="btn-tertiary"
            >
              {themeMode === 'light' ? '🌙 Dark Mode' : '☀️ Light Mode'}
            </button>
            <span className="text-muted">|</span>
            <select
              value={season}
              onChange={e => setSeason(e.target.value as any)}
              style={{ background: 'transparent', color: 'var(--accent-primary)', border: 'none', fontSize: '12px', cursor: 'pointer', outline: 'none' }}
            >
              <option value="spring">Spring</option>
              <option value="summer">Summer</option>
              <option value="autumn">Autumn</option>
              <option value="winter">Winter</option>
            </select>
          </div>

          {/* Account Actions */}
          <div style={{ display: 'flex', justifyContent: 'center' }}>
            <button onClick={deleteAccount} className="btn-tertiary" style={{ fontSize: '11px', opacity: 0.7 }}>Delete Account</button>
          </div>

          <div style={{ padding: '16px', flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column', gap: '8px', borderTop: '1px solid var(--surface-border)' }}>
            <h3 style={{ fontSize: '12px', textTransform: 'uppercase', color: 'var(--text-muted)', flexShrink: 0 }}>Activity Log</h3>
            <div style={{ background: '#000', borderRadius: '8px', padding: '12px', fontSize: '12px', fontFamily: 'var(--font-mono)', display: 'flex', flexDirection: 'column', gap: '10px', overflowY: 'auto', flex: 1, minHeight: 0, border: '1px solid rgba(255,255,255,0.1)' }}>
              {logs.map((l, i) => (
                <div key={i} style={{ color: l.type === 'err' ? 'var(--accent-danger)' : l.type === 'ok' ? 'var(--accent-success)' : 'var(--text-muted)', borderBottom: i < logs.length - 1 ? '1px solid rgba(255,255,255,0.05)' : 'none', paddingBottom: i < logs.length - 1 ? '8px' : '0' }}>
                  <div style={{ opacity: 0.5, fontSize: '10px', marginBottom: '4px' }}>{l.ts}</div>
                  <span style={{ wordBreak: 'break-all' }}>{l.msg}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* Main Content Area */}
      <div className="main-content">
        <div className="header-area flex-row space-between">
          <div className="flex-row">
            <div className={`led-indicator ${statusOk ? 'online' : 'offline'}`}></div>
            <span style={{ fontFamily: 'var(--font-display)', fontWeight: 600 }}>
              {statusOk ? 'ONLINE' : 'DISCONNECTED'}
            </span>
          </div>

          <div style={{ display: 'flex', gap: '16px' }}>
            <span
              style={{ cursor: 'pointer', color: section === 'query' ? 'var(--text-main)' : 'var(--text-muted)', fontWeight: section === 'query' ? 600 : 400 }}
              onClick={() => setSection('query')}
            >
              SQL Studio
            </span>
            <span
              style={{ cursor: 'pointer', color: section === 'schema' ? 'var(--text-main)' : 'var(--text-muted)', fontWeight: section === 'schema' ? 600 : 400 }}
              onClick={() => { setSection('schema'); loadSchema(); }}
            >
              Catalog
            </span>
            <span
              style={{ cursor: 'pointer', color: section === 'ingest' ? 'var(--text-main)' : 'var(--text-muted)', fontWeight: section === 'ingest' ? 600 : 400 }}
              onClick={() => setSection('ingest')}
            >
              Data Loader
            </span>
            <span
              style={{ cursor: 'pointer', color: section === 'secrets' ? 'var(--text-main)' : 'var(--text-muted)', fontWeight: section === 'secrets' ? 600 : 400 }}
              onClick={() => setSection('secrets')}
            >
              Connections
            </span>
            <span
              style={{ cursor: 'pointer', color: section === 'team' ? 'var(--text-main)' : 'var(--text-muted)', fontWeight: section === 'team' ? 600 : 400 }}
              onClick={() => { setSection('team'); loadOrgs(); }}
            >
              Team
            </span>
            <span
              style={{ cursor: 'pointer', color: section === 'dbt' ? 'var(--text-main)' : 'var(--text-muted)', fontWeight: section === 'dbt' ? 600 : 400 }}
              onClick={() => { setSection('dbt'); loadDbtJobs(); }}
            >
              dbt
            </span>
            <span
              style={{ cursor: 'pointer', color: section === 'support' ? 'var(--text-main)' : 'var(--text-muted)', fontWeight: section === 'support' ? 600 : 400 }}
              onClick={() => setSection('support')}
            >
              Support
            </span>
          </div>
        </div>

        {section === 'query' && (
          <>
            <div className="query-editor-wrapper">
              <div style={{ position: 'relative' }}>
                <textarea
                  ref={editorRef}
                  className="code-editor"
                  value={querySql}
                  onChange={handleEditorInput}
                  onKeyDown={handleEditorKeyDown}
                  onBlur={() => setTimeout(() => setShowSuggestions(false), 150)}
                  spellCheck={false}
                  placeholder="SELECT * FROM my_table LIMIT 100"
                />
                {showSuggestions && suggestions.length > 0 && (
                  <div style={{
                    position: 'absolute', bottom: 0, left: 16,
                    background: 'rgba(8, 20, 38, 0.97)', border: '1px solid var(--surface-border-strong)',
                    borderRadius: '8px', overflow: 'hidden', zIndex: 100,
                    boxShadow: '0 8px 32px rgba(0,0,0,0.5)', minWidth: '220px',
                    backdropFilter: 'blur(12px)'
                  }}>
                    {suggestions.map((s, i) => (
                      <div
                        key={s}
                        onMouseDown={() => applySuggestion(s)}
                        style={{
                          padding: '8px 14px', cursor: 'pointer', fontSize: '13px',
                          fontFamily: 'var(--font-mono)',
                          background: i === suggestionIdx ? 'rgba(0, 153, 204, 0.25)' : 'transparent',
                          color: i === suggestionIdx ? 'var(--text-main)' : 'var(--text-muted)',
                          borderLeft: i === suggestionIdx ? '3px solid var(--accent-primary)' : '3px solid transparent',
                          transition: 'all 0.1s'
                        }}
                      >
                        {s}
                      </div>
                    ))}
                  </div>
                )}
              </div>
              <div className="flex-row space-between" style={{ alignItems: 'center' }}>
                <div className="flex-row" style={{ gap: '12px' }}>
                  <select
                    value={computeTier}
                    onChange={e => setComputeTier(e.target.value as any)}
                    style={{ background: 'var(--surface-glass)', border: '1px solid var(--surface-border)', color: 'var(--text-main)', padding: '8px 12px', borderRadius: '6px', outline: 'none', fontFamily: 'var(--font-display)', cursor: 'pointer' }}
                  >
                    <option value="micro">Micro Warehouse (256MB)</option>
                    <option value="standard">Standard Warehouse (2GB)</option>
                    <option value="enterprise">Enterprise Warehouse (16GB)</option>
                  </select>
                  {queryRunning ? (
                    <button
                      className="btn-primary"
                      onClick={abortQuery}
                      style={{ background: 'var(--accent-danger)', border: '1px solid var(--accent-danger)' }}
                    >
                      ■ Abort Query
                    </button>
                  ) : (
                    <button
                      className="btn-primary"
                      onClick={runQuery}
                    >
                      Run Query ▶
                    </button>
                  )}
                </div>
                <div className="text-muted" style={{ fontSize: '13px', fontFamily: 'var(--font-mono)' }}>
                  {queryStatus || 'Ready'}
                </div>
              </div>
            </div>

            {results && (
              <>
                <div style={{ padding: '0 24px', display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
                  <div className="text-muted" style={{ fontSize: '13px' }}>
                    {results.row_count.toLocaleString()} rows × {results.columns.length} columns
                  </div>
                  <div style={{ position: 'relative' }}>
                    <button
                      className="btn-primary"
                      onClick={() => !exporting && setExportMenuOpen(v => !v)}
                      disabled={exporting}
                      style={{ gap: '6px', fontSize: '12px', padding: '8px 20px', position: 'relative', overflow: 'hidden' }}
                    >
                      {exporting ? (
                        <>
                          <div className="spinner-mini" />
                          Generating...
                        </>
                      ) : (
                        <>↓ Export As...</>
                      )}
                    </button>
                    {exportMenuOpen && (
                      <div style={{
                        position: 'absolute', top: 'calc(100% + 8px)', right: 0,
                        background: 'var(--surface-glass)', backdropFilter: 'blur(12px)',
                        border: '1px solid var(--surface-border)', borderRadius: '12px',
                        padding: '6px', zIndex: 100, minWidth: '160px',
                        boxShadow: '0 8px 24px rgba(0,0,0,0.3)'
                      }}>
                        {[['csv', 'CSV Document'], ['json', 'JSON Array'], ['parquet', 'Parquet (Binary)']].map(([fmt, label]) => (
                          <button key={fmt} onClick={() => { exportResults(fmt); setExportMenuOpen(false); }} style={{
                            display: 'block', width: '100%', textAlign: 'left',
                            background: 'none', border: 'none', color: 'var(--text-main)',
                            padding: '8px 12px', borderRadius: '8px', cursor: 'pointer',
                            fontSize: '13px', fontFamily: 'var(--font-display)', fontWeight: 500,
                            transition: 'background 0.15s'
                          }}
                          onMouseEnter={e => { e.currentTarget.style.background = 'var(--surface-glass-hover)'; }}
                          onMouseLeave={e => { e.currentTarget.style.background = 'none'; }}
                          >{label}</button>
                        ))}
                      </div>
                    )}
                  </div>
                </div>
                <div className="data-grid-container">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th style={{ width: '50px', textAlign: 'center', color: 'var(--text-muted)', fontSize: '10px' }}>#</th>
                        {results.columns.map((c: string) => <th key={c}>{c}</th>)}
                      </tr>
                    </thead>
                    <tbody>
                      {results.rows.map((row: any, i: number) => (
                        <tr key={i}>
                          <td style={{ textAlign: 'center', color: 'var(--text-muted)', fontSize: '11px', opacity: 0.5 }}>{i + 1}</td>
                          {results.columns.map((c: string) => <td key={`${i}-${c}`}>{String(row[c] ?? '')}</td>)}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </>
        )}

        {section === 'schema' && (
          <div className="query-editor-wrapper" style={{ overflow: 'auto', flex: 1 }}>
            <h2 style={{ marginBottom: '16px' }}>S3 Lakehouse Catalog</h2>
            <div style={{ display: 'flex', gap: '24px', flex: 1, minHeight: 0 }}>
              {/* Left Pane: Table List */}
              <div className="glass-panel" style={{ width: '280px', display: 'flex', flexDirection: 'column', flexShrink: 0, padding: 0, overflow: 'hidden' }}>
                <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--surface-border)', background: 'rgba(0,0,0,0.2)' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '10px' }}>
                    <h3 style={{ margin: 0, fontSize: '12px', textTransform: 'uppercase', color: 'var(--text-muted)', letterSpacing: '0.05em' }}>Tables</h3>
                    <span style={{ fontSize: '11px', padding: '2px 8px', borderRadius: '9999px', background: 'rgba(0,153,204,0.15)', color: 'var(--accent-secondary)' }}>{schemaTables.length}</span>
                  </div>
                  <input
                    type="text"
                    placeholder="Filter tables..."
                    value={catalogFilter}
                    onChange={e => setCatalogFilter(e.target.value)}
                    style={{
                      width: '100%', padding: '7px 12px', borderRadius: '6px',
                      background: 'rgba(0,0,0,0.3)', border: '1px solid var(--surface-border)',
                      color: 'var(--text-main)', fontSize: '12px', outline: 'none',
                      fontFamily: 'var(--font-body)', transition: 'border-color 0.2s'
                    }}
                    onFocus={e => e.currentTarget.style.borderColor = 'var(--accent-primary)'}
                    onBlur={e => e.currentTarget.style.borderColor = 'var(--surface-border)'}
                  />
                </div>
                <div style={{ flex: 1, overflowY: 'auto' }}>
                  {schemaTables.filter(t => !catalogFilter || t.name.toLowerCase().includes(catalogFilter.toLowerCase())).map(t => (
                    <div
                      key={t.name}
                      onClick={() => setSelectedTable(t.name)}
                      style={{
                        padding: '10px 16px',
                        cursor: 'pointer',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '10px',
                        borderBottom: '1px solid rgba(255,255,255,0.03)',
                        background: selectedTable === t.name ? 'rgba(0, 153, 204, 0.15)' : 'transparent',
                        borderLeft: selectedTable === t.name ? '3px solid var(--accent-primary)' : '3px solid transparent',
                        transition: 'all 0.15s'
                      }}
                      onMouseEnter={e => { if (selectedTable !== t.name) e.currentTarget.style.background = 'rgba(255,255,255,0.03)' }}
                      onMouseLeave={e => { if (selectedTable !== t.name) e.currentTarget.style.background = 'transparent' }}
                    >
                      <span style={{ fontSize: '1.1em', opacity: 0.8 }}>{t.type === 'view' ? '👁️' : '🗄️'}</span>
                      <div style={{ flex: 1, minWidth: 0 }}>
                        <div style={{ fontSize: '13px', fontWeight: 500, color: selectedTable === t.name ? 'var(--accent-primary)' : 'var(--text-main)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{t.name}</div>
                        <div style={{ fontSize: '11px', color: 'var(--text-muted)', marginTop: '2px' }}>
                          {t.columns?.length || 0} cols{t.metrics?.total_rows > 0 ? ` · ${t.metrics.total_rows.toLocaleString()} rows` : ''}
                        </div>
                      </div>
                      {t.metrics?.total_size_bytes > 0 && (
                        <span style={{ fontSize: '10px', color: 'var(--text-muted)', whiteSpace: 'nowrap', opacity: 0.6 }}>
                          {t.metrics.total_size_bytes > 1048576 ? (t.metrics.total_size_bytes / 1048576).toFixed(1) + ' MB' : (t.metrics.total_size_bytes / 1024).toFixed(0) + ' KB'}
                        </span>
                      )}
                    </div>
                  ))}
                  {schemaTables.length === 0 && <div className="text-muted" style={{ padding: '24px 16px', textAlign: 'center', fontSize: '13px' }}>No tables found.<br />Create tables with SQL or upload files.</div>}
                </div>
              </div>

              {/* Right Pane: Table Details */}
              <div style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
                {selectedTable && schemaTables.find(t => t.name === selectedTable) ? (() => {
                  const t = schemaTables.find(t => t.name === selectedTable);
                  return (
                    <div className="glass-panel" style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
                      <div style={{ padding: '20px 24px', borderBottom: '1px solid var(--surface-border)', background: 'rgba(0,0,0,0.15)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <div>
                          <h2 style={{ margin: '0 0 4px 0', display: 'flex', alignItems: 'center', gap: '10px' }}>
                            <span style={{ fontSize: '1.2em' }}>{t.type === 'view' ? '👁️' : '🗄️'}</span> {t.name}
                          </h2>
                          <div className="text-muted" style={{ fontSize: '13px' }}>
                            {t.type === 'view' ? 'Iceberg Logical View' : `Iceberg V${t.properties?.["format-version"] || 1} Table`}
                          </div>
                        </div>
                        <button
                          onClick={() => { dropTable(t.name); setSelectedTable(null); }}
                          title="Delete permanently"
                          style={{ background: 'rgba(239, 68, 68, 0.1)', border: '1px solid rgba(239, 68, 68, 0.3)', borderRadius: '6px', padding: '8px 16px', cursor: 'pointer', color: 'var(--accent-danger)', fontSize: '13px', fontWeight: 600, transition: 'all 0.2s' }}
                          onMouseEnter={e => { e.currentTarget.style.background = 'rgba(239, 68, 68, 0.2)'; }}
                          onMouseLeave={e => { e.currentTarget.style.background = 'rgba(239, 68, 68, 0.1)'; }}
                        >Drop {t.type === 'view' ? 'View' : 'Table'}</button>
                      </div>

                      <div style={{ flex: 1, overflowY: 'auto', padding: '24px', display: 'flex', flexDirection: 'column', gap: '24px' }}>
                        {/* Metrics Row (Only for Physical Tables) */}
                        {t.type !== 'view' && (
                          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '16px' }}>
                            <div style={{ background: 'rgba(0,0,0,0.2)', border: '1px solid var(--surface-border-strong)', borderRadius: '8px', padding: '16px' }}>
                              <div className="text-muted" style={{ fontSize: '11px', textTransform: 'uppercase', marginBottom: '8px' }}>Total Rows</div>
                              <div style={{ fontSize: '24px', fontFamily: 'var(--font-display)', fontWeight: 600 }}>{t.metrics?.total_rows?.toLocaleString() || 0}</div>
                            </div>
                            <div style={{ background: 'rgba(0,0,0,0.2)', border: '1px solid var(--surface-border-strong)', borderRadius: '8px', padding: '16px' }}>
                              <div className="text-muted" style={{ fontSize: '11px', textTransform: 'uppercase', marginBottom: '8px' }}>Table Size</div>
                              <div style={{ fontSize: '24px', fontFamily: 'var(--font-display)', fontWeight: 600 }}>{t.metrics?.total_size_bytes ? (t.metrics.total_size_bytes / 1024).toFixed(1) + ' KB' : '0 KB'}</div>
                            </div>
                            <div style={{ background: 'rgba(0,0,0,0.2)', border: '1px solid var(--surface-border-strong)', borderRadius: '8px', padding: '16px' }}>
                              <div className="text-muted" style={{ fontSize: '11px', textTransform: 'uppercase', marginBottom: '8px' }}>Data Files</div>
                              <div style={{ fontSize: '24px', fontFamily: 'var(--font-display)', fontWeight: 600 }}>{t.metrics?.total_data_files?.toLocaleString() || 0}</div>
                            </div>
                          </div>
                        )}

                        {/* Schema / Definition display block */}
                        <div>
                          <h3 style={{ fontSize: '14px', marginBottom: '12px', color: 'var(--text-muted)' }}>Schema Definitions</h3>
                          <div style={{ border: '1px solid var(--surface-border)', borderRadius: '8px', overflow: 'hidden', background: 'rgba(0,0,0,0.1)' }}>
                            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                              <thead>
                                <tr>
                                  <th style={{ padding: '10px 16px', fontSize: '12px', textAlign: 'left', borderBottom: '1px solid var(--surface-border)', background: 'rgba(0,0,0,0.2)' }}>Column Name</th>
                                  <th style={{ padding: '10px 16px', fontSize: '12px', textAlign: 'right', borderBottom: '1px solid var(--surface-border)', background: 'rgba(0,0,0,0.2)' }}>Data Type</th>
                                </tr>
                              </thead>
                              <tbody>
                                {t.columns?.map((col: any, i: number) => (
                                  <tr key={col.name} style={{ background: i % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.02)' }}>
                                    <td style={{ padding: '8px 16px', fontSize: '13px', fontFamily: 'var(--font-mono)', borderBottom: '1px solid rgba(255,255,255,0.03)' }}>{col.name}</td>
                                    <td style={{ padding: '8px 16px', fontSize: '13px', fontFamily: 'var(--font-mono)', color: 'var(--accent-primary)', textAlign: 'right', borderBottom: '1px solid rgba(255,255,255,0.03)' }}>{col.type}</td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                            {(!t.columns || t.columns.length === 0) && <div className="text-muted" style={{ padding: '16px', textAlign: 'center' }}>No schema available</div>}
                          </div>
                        </div>

                        {/* Snapshot History */}
                        {t.type !== 'view' && t.history?.length > 0 && (
                          <div>
                            <h3 style={{ fontSize: '14px', marginBottom: '12px', color: 'var(--text-muted)' }}>Snapshot History</h3>
                            <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                              {t.history.map((snap: any, i: number) => (
                                <div key={i} style={{ padding: '12px 16px', background: 'rgba(0,0,0,0.15)', border: '1px solid var(--surface-border)', borderRadius: '6px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                  <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                                    <span style={{ fontSize: '11px', padding: '4px 10px', borderRadius: '9999px', background: 'rgba(0, 153, 204, 0.15)', color: 'var(--accent-secondary)', textTransform: 'uppercase', fontWeight: 600 }}>{snap.operation || 'commit'}</span>
                                    <span style={{ fontSize: '13px' }}>{new Date(snap.timestamp_ms).toLocaleString()}</span>
                                  </div>
                                  <div className="text-muted" style={{ fontSize: '12px', fontFamily: 'var(--font-mono)' }}>
                                    {snap.summary?.['added-data-files'] ? `+${snap.summary['added-data-files']} files` : ''}
                                    {snap.summary?.['added-records'] ? ` (+${snap.summary['added-records']} rows)` : ''}
                                  </div>
                                </div>
                              ))}
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                  );
                })() : (
                  <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', flexDirection: 'column', gap: '16px' }}>
                    <div style={{ fontSize: '3em', opacity: 0.5 }}>🗂️</div>
                    <div className="text-muted" style={{ fontSize: '15px' }}>Select a table to view its native Iceberg metadata and schema.</div>
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {section === 'ingest' && (
          <div className="query-editor-wrapper" style={{ maxWidth: '680px', flex: 1, paddingBottom: '40px', minHeight: 0, gap: '12px' }}>
            <h2 style={{ marginBottom: '4px' }}>Data Loader</h2>
            <p className="text-muted" style={{ marginBottom: '24px' }}>Ingest datasets from public URLs or upload local files directly into your Iceberg Lakehouse.</p>

            {/* Remote URL Ingestion */}
            <div className="glass-panel" style={{ padding: '24px 28px', display: 'flex', flexDirection: 'column', gap: '12px' }}>
              <h3 style={{ fontSize: '13px', textTransform: 'uppercase', color: 'var(--text-muted)', letterSpacing: '0.05em', margin: 0 }}>Remote Source</h3>
              <input type="text" className="code-editor" placeholder="https://... or s3://source-bucket/data/*.parquet" value={ingestForm.uri} onChange={e => setIngestForm({ ...ingestForm, uri: e.target.value })} style={{ minHeight: '40px', padding: '12px' }} />
              <input type="text" className="code-editor" placeholder="Target table name (e.g. taxi_data)" value={ingestForm.table} onChange={e => setIngestForm({ ...ingestForm, table: e.target.value })} style={{ minHeight: '40px', padding: '12px' }} />
              <div style={{ display: 'grid', gridTemplateColumns: '1fr auto', gap: '12px', alignItems: 'center' }}>
                <select
                  value={ingestForm.tier}
                  onChange={e => setIngestForm({ ...ingestForm, tier: e.target.value })}
                  style={{ background: 'var(--surface-glass)', border: '1px solid var(--surface-border)', color: 'var(--text-main)', padding: '10px 12px', borderRadius: '6px', outline: 'none', cursor: 'pointer', fontSize: '13px' }}
                >
                  <option value="micro">Micro ETL (256MB)</option>
                  <option value="standard">Standard ETL (2GB)</option>
                  <option value="enterprise">Enterprise ETL (16GB)</option>
                </select>
                <button className="btn-primary" onClick={startIngest}>Initiate Cloud Mirror</button>
              </div>
            </div>

            {/* Local File Upload */}
            <div className="glass-panel" style={{ padding: '24px 28px', display: 'flex', flexDirection: 'column', gap: '12px' }}>
              <h3 style={{ fontSize: '13px', textTransform: 'uppercase', color: 'var(--text-muted)', letterSpacing: '0.05em', margin: 0 }}>Local Upload</h3>
              <div
                id="file-drop-zone"
                onDragOver={e => { e.preventDefault(); (e.currentTarget as HTMLElement).style.borderColor = 'var(--accent-primary)'; }}
                onDragLeave={e => { (e.currentTarget as HTMLElement).style.borderColor = 'var(--surface-border-strong)'; }}
                onDrop={e => {
                  e.preventDefault();
                  (e.currentTarget as HTMLElement).style.borderColor = 'var(--surface-border-strong)';
                  const file = e.dataTransfer.files[0];
                  if (file) handleFileUpload(file);
                }}
                onClick={() => document.getElementById('file-input')?.click()}
                style={{
                  border: '2px dashed var(--surface-border-strong)',
                  borderRadius: '10px',
                  padding: '16px',
                  textAlign: 'center',
                  cursor: 'pointer',
                  transition: 'all 0.2s ease',
                  background: 'rgba(0,0,0,0.15)',
                  maxHeight: 'min(15vh, 160px)',
                  display: 'flex',
                  flexDirection: 'column',
                  justifyContent: 'center'
                }}
              >
                <div style={{ fontSize: '1.5em', marginBottom: '4px' }}>📁</div>
                <div style={{ fontWeight: 500, fontSize: '14px' }}>Drop parquet or csv files</div>
                <div className="text-muted" style={{ fontSize: '12px' }}>or click to browse</div>
              </div>
              <input
                id="file-input"
                type="file"
                accept=".parquet,.csv,.json,.avro"
                style={{ display: 'none' }}
                onChange={e => { if (e.target.files?.[0]) handleFileUpload(e.target.files[0]); }}
              />
              <input type="text" className="code-editor" placeholder="Target table name for upload" value={uploadTable} onChange={e => setUploadTable(e.target.value)} style={{ minHeight: '40px', padding: '12px' }} />
              {uploadStatus && <span className="text-muted" style={{ fontSize: '13px' }}>{uploadStatus}</span>}
            </div>

            {ingestStatus && <span className="text-muted" style={{ marginTop: '8px' }}>{ingestStatus}</span>}
          </div>
        )}

        {section === 'secrets' && (
          <div className="query-editor-wrapper" style={{ maxWidth: '800px', flex: 1, paddingBottom: '40px' }}>
            <h2 style={{ marginBottom: '16px' }}>Federated Connections</h2>
            <p className="text-muted" style={{ marginBottom: '24px' }}>Configure your external Zero-Trust database endpoints natively. shikipond transmits these securely on execution over TLS without persisting any credentials physically on the Backend Coordinator disk.</p>

            <div className="glass-panel" style={{ padding: '32px', display: 'flex', flexDirection: 'column', gap: '16px', marginBottom: '24px' }}>
              <h3 style={{ margin: 0 }}>Add New Connection</h3>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px' }}>
                <input type="text" placeholder="Friendly Name (e.g. remote_pg)" value={newSecret.name} onChange={e => setNewSecret({ ...newSecret, name: e.target.value })} style={{ padding: '8px', borderRadius: '4px', border: '1px solid var(--surface-border)', background: 'var(--surface-glass)', color: 'var(--text-main)' }} />
                <select value={newSecret.type} onChange={e => setNewSecret({ ...newSecret, type: e.target.value })} style={{ padding: '8px', borderRadius: '4px', border: '1px solid var(--surface-border)', background: 'var(--surface-glass)', color: 'var(--text-main)' }}>
                  <option value="POSTGRES">PostgreSQL</option>
                  <option value="MYSQL">MySQL</option>
                  <option value="GCS">Google Cloud Storage (GCP)</option>
                  <option value="AZURE">Microsoft Azure Blob</option>
                  <option value="MSSQL">SQL Server (Via ODBC)</option>
                </select>
                <input type="text" placeholder="Host / Endpoint URL" value={newSecret.host} onChange={e => setNewSecret({ ...newSecret, host: e.target.value })} style={{ padding: '8px', borderRadius: '4px', border: '1px solid var(--surface-border)', background: 'var(--surface-glass)', color: 'var(--text-main)' }} />
                <input type="text" placeholder="Port (e.g. 5432)" value={newSecret.port} onChange={e => setNewSecret({ ...newSecret, port: e.target.value })} style={{ padding: '8px', borderRadius: '4px', border: '1px solid var(--surface-border)', background: 'var(--surface-glass)', color: 'var(--text-main)' }} />
                <input type="text" placeholder="Database User" value={newSecret.user} onChange={e => setNewSecret({ ...newSecret, user: e.target.value })} style={{ padding: '8px', borderRadius: '4px', border: '1px solid var(--surface-border)', background: 'var(--surface-glass)', color: 'var(--text-main)' }} />
                <input type="password" placeholder="Password / Token" value={newSecret.pass} onChange={e => setNewSecret({ ...newSecret, pass: e.target.value })} style={{ padding: '8px', borderRadius: '4px', border: '1px solid var(--surface-border)', background: 'var(--surface-glass)', color: 'var(--text-main)' }} />
              </div>
              <button className="btn-primary" style={{ alignSelf: 'flex-start' }} onClick={() => {
                if (!newSecret.host) return;
                const arr = [...(cfg.secrets || []), { ...newSecret }];
                const newCfg = { ...cfg, secrets: arr };
                setCfg(newCfg);
                localStorage.setItem('duckpond_cfg', JSON.stringify(newCfg));
                addLog('Secret mapped securely to LocalStorage', 'ok');
              }}>Bind Connection</button>
            </div>

            <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
              {(cfg.secrets || []).map((s, idx) => (
                <div key={idx} className="glass-panel" style={{ padding: '16px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <div>
                    <h3 style={{ margin: '0 0 4px 0' }}>{s.name} <span style={{ fontSize: '12px', opacity: 0.5, marginLeft: '8px' }}>{s.type}</span></h3>
                    <p style={{ margin: 0, fontSize: '13px', color: 'var(--text-muted)' }}>{s.host}:{s.port} as {s.user}</p>
                  </div>
                  <button className="btn-primary" style={{ background: 'var(--accent-danger)', border: 'none' }} onClick={() => {
                    const arr = [...(cfg.secrets || [])];
                    arr.splice(idx, 1);
                    const newCfg = { ...cfg, secrets: arr };
                    setCfg(newCfg);
                    localStorage.setItem('duckpond_cfg', JSON.stringify(newCfg));
                  }}>Remove</button>
                </div>
              ))}
            </div>
          </div>
        )}

        {section === 'team' && (
          <div className="query-editor-wrapper" style={{ maxWidth: '680px' }}>
            <h2 style={{ marginBottom: '4px' }}>Organization</h2>
            <p className="text-muted" style={{ marginBottom: '24px' }}>Create or join a team to share your Iceberg lakehouse with collaborators.</p>

            {orgs.length === 0 ? (
              <div className="glass-panel" style={{ padding: '32px 28px', display: 'flex', flexDirection: 'column', gap: '14px' }}>
                <h3 style={{ fontSize: '13px', textTransform: 'uppercase', color: 'var(--text-muted)', letterSpacing: '0.05em', margin: 0 }}>Create Organization</h3>
                <input type="text" className="code-editor" placeholder="Organization name (e.g. Acme Inc.)" value={newOrgName} onChange={e => { setNewOrgName(e.target.value); setNewOrgSlug(e.target.value.toLowerCase().replace(/[^a-z0-9]/g, '_').replace(/_+/g, '_').replace(/^_|_$/g, '')); }} style={{ minHeight: '40px', padding: '12px' }} />
                <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
                  <span className="text-muted" style={{ fontSize: '13px', whiteSpace: 'nowrap' }}>Slug:</span>
                  <input type="text" className="code-editor" placeholder="acme" value={newOrgSlug} onChange={e => setNewOrgSlug(e.target.value.toLowerCase().replace(/[^a-z0-9_]/g, ''))} style={{ minHeight: '36px', padding: '8px 12px', flex: 1 }} />
                </div>
                <button className="btn-primary" onClick={createOrg} style={{ alignSelf: 'flex-start' }}>Create Organization</button>
                {orgStatus && <span className="text-muted" style={{ fontSize: '13px' }}>{orgStatus}</span>}
              </div>
            ) : (
              orgs.map(org => (
                <div key={org.id}>
                  <div className="glass-panel" style={{ padding: '20px', marginBottom: '16px' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '16px' }}>
                      <div style={{ width: '40px', height: '40px', borderRadius: '10px', background: 'linear-gradient(135deg, var(--accent-primary), #8B5CF6)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '18px', fontWeight: 700, color: 'white' }}>
                        {org.name[0].toUpperCase()}
                      </div>
                      <div>
                        <div style={{ fontWeight: 600, fontSize: '1.1em', fontFamily: 'var(--font-display)' }}>{org.name}</div>
                        <div className="text-muted" style={{ fontSize: '12px' }}>Namespace: <span style={{ color: 'var(--accent-primary)' }}>org_{org.slug}</span> · {org.role}</div>
                      </div>
                    </div>

                    <h4 style={{ fontSize: '12px', textTransform: 'uppercase', color: 'var(--text-muted)', letterSpacing: '0.05em', marginBottom: '12px' }}>Members</h4>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', marginBottom: '16px' }}>
                      {orgMembers.map(m => (
                        <div key={m.email} style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '8px 12px', borderRadius: '6px', background: 'rgba(0,0,0,0.15)' }}>
                          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                            <div style={{ width: '28px', height: '28px', borderRadius: '50%', background: m.role === 'admin' ? '#8B5CF6' : 'var(--surface-border-strong)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '12px', fontWeight: 600 }}>
                              {m.email[0].toUpperCase()}
                            </div>
                            <span style={{ fontSize: '13px' }}>{m.email}</span>
                            <span style={{ fontSize: '11px', padding: '2px 8px', borderRadius: '9999px', background: m.role === 'admin' ? 'rgba(139,92,246,0.2)' : 'rgba(255,255,255,0.05)', color: m.role === 'admin' ? '#A78BFA' : 'var(--text-muted)' }}>{m.role}</span>
                          </div>
                          {org.role === 'admin' && m.email !== session.user.email && (
                            <button onClick={() => removeMember(org.slug, m.email)} style={{ background: 'none', border: 'none', color: 'var(--accent-danger)', cursor: 'pointer', fontSize: '12px', opacity: 0.6 }}>Remove</button>
                          )}
                        </div>
                      ))}
                    </div>

                    {org.role === 'admin' && (
                      <div style={{ display: 'flex', gap: '8px' }}>
                        <input type="email" className="code-editor" placeholder="Invite by email..." value={inviteEmail} onChange={e => setInviteEmail(e.target.value)} onKeyDown={e => e.key === 'Enter' && inviteMember(org.slug)} style={{ minHeight: '36px', padding: '8px 12px', flex: 1 }} />
                        <button className="btn-primary" onClick={() => inviteMember(org.slug)} style={{ whiteSpace: 'nowrap' }}>Invite</button>
                      </div>
                    )}
                    {orgStatus && <div className="text-muted" style={{ fontSize: '13px', marginTop: '8px' }}>{orgStatus}</div>}

                    {org.role === 'admin' && (
                      <div style={{ marginTop: '24px', paddingTop: '16px', borderTop: '1px solid rgba(255,255,255,0.06)' }}>
                        <h4 style={{ fontSize: '12px', textTransform: 'uppercase', color: 'var(--accent-danger)', letterSpacing: '0.05em', marginBottom: '8px', opacity: 0.7 }}>Danger Zone</h4>
                        <button onClick={() => deleteOrg(org.slug, org.name)} style={{ background: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.3)', color: 'var(--accent-danger)', cursor: 'pointer', padding: '8px 16px', borderRadius: '6px', fontSize: '13px' }}>
                          Delete Organization
                        </button>
                      </div>
                    )}
                  </div>
                </div>
              ))
            )}
          </div>
        )}

        {section === 'dbt' && (
          <div className="query-editor-wrapper" style={{ maxWidth: '800px', flex: 1, overflowY: 'auto' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '4px' }}>
              <h2 style={{ margin: 0 }}>dbt</h2>
              {dbtRunning && <div className="led-indicator online" style={{ width: '8px', height: '8px' }}></div>}
              {dbtStatus && !dbtRunning && (
                <span style={{ fontSize: '12px', padding: '2px 10px', borderRadius: '9999px', background: dbtStatus === 'success' ? 'rgba(16,185,129,0.15)' : dbtStatus === 'failed' ? 'rgba(239,68,68,0.15)' : 'rgba(255,255,255,0.05)', color: dbtStatus === 'success' ? 'var(--accent-success)' : dbtStatus === 'failed' ? 'var(--accent-danger)' : 'var(--text-muted)' }}>{dbtStatus}</span>
              )}
            </div>
            <p className="text-muted" style={{ marginBottom: '20px' }}>Run dbt models on your shikipond lakehouse. Models materialize as queryable tables.</p>

            <div className="glass-panel" style={{ padding: '32px 28px', display: 'flex', flexDirection: 'column', gap: '16px', marginBottom: '16px' }}>
              <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
                <h3 style={{ fontSize: '12px', textTransform: 'uppercase', color: 'var(--text-muted)', margin: 0, whiteSpace: 'nowrap' }}>Source</h3>
                <div style={{ display: 'flex', gap: '0', borderRadius: '6px', overflow: 'hidden', border: '1px solid var(--surface-border)', flex: 1 }}>
                  {(['git', 'upload'] as const).map(s => (
                    <button key={s} onClick={() => setDbtSource(s)} style={{ flex: 1, padding: '6px', border: 'none', cursor: 'pointer', fontSize: '12px', fontWeight: 500, background: dbtSource === s ? 'var(--accent-primary)' : 'var(--surface-glass)', color: dbtSource === s ? 'white' : 'var(--text-main)', transition: 'background 0.2s' }}>
                      {s === 'git' ? 'Git URL' : 'Upload Zip'}
                    </button>
                  ))}
                </div>
              </div>

              {dbtSource === 'git' && (
                <input type="text" className="code-editor" placeholder="https://github.com/your-org/your-dbt-project.git" value={dbtGitUrl} onChange={e => setDbtGitUrl(e.target.value)} style={{ minHeight: '36px', padding: '8px 12px' }} />
              )}

              {dbtSource === 'upload' && (
                <div style={{ padding: '16px', border: '1px dashed var(--surface-border)', borderRadius: '8px', textAlign: 'center' }}>
                  <input type="file" accept=".zip" id="dbt-upload" style={{ display: 'none' }} onChange={e => { if (e.target.files?.[0]) uploadDbtProject(e.target.files[0]); }} />
                  <label htmlFor="dbt-upload" style={{ cursor: 'pointer', color: 'var(--text-muted)', fontSize: '13px' }}>Click to select a <strong>.zip</strong> file containing your dbt project</label>
                </div>
              )}

              <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
                <select value={dbtCommand} onChange={e => setDbtCommand(e.target.value)} style={{ padding: '8px 12px', borderRadius: '6px', border: '1px solid var(--surface-border)', background: 'var(--surface-glass)', color: 'var(--text-main)', fontSize: '13px' }}>
                  <option value="build">dbt build</option>
                  <option value="run">dbt run</option>
                  <option value="test">dbt test</option>
                  <option value="seed">dbt seed</option>
                  <option value="compile">dbt compile</option>
                </select>
                <button className="btn-primary" onClick={runDbt} disabled={dbtRunning || (dbtSource === 'upload')} style={{ alignSelf: 'flex-start', opacity: dbtRunning ? 0.6 : 1 }}>
                  {dbtRunning ? 'Running...' : `Run ${dbtCommand}`}
                </button>
              </div>
            </div>

            {/* Log Output */}
            {dbtLogs.length > 0 && (
              <div className="glass-panel" style={{ padding: '16px', marginBottom: '16px' }}>
                <h3 style={{ fontSize: '12px', textTransform: 'uppercase', color: 'var(--text-muted)', marginBottom: '12px' }}>Output</h3>
                <div style={{ fontFamily: 'var(--font-mono)', fontSize: '12px', lineHeight: '1.6', maxHeight: '400px', overflow: 'auto', background: 'rgba(0,0,0,0.3)', padding: '12px', borderRadius: '6px' }}>
                  {dbtLogs.map((line, i) => (
                    <div key={i} style={{ color: line.includes('PASS') || line.includes('OK') ? 'var(--accent-success)' : line.includes('FAIL') || line.includes('ERROR') ? 'var(--accent-danger)' : line.includes('WARN') ? '#F59E0B' : 'var(--text-muted)', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
                      {line}
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Materialized Tables */}
            {dbtTables.length > 0 && (
              <div className="glass-panel" style={{ padding: '16px', marginBottom: '16px' }}>
                <h3 style={{ fontSize: '12px', textTransform: 'uppercase', color: 'var(--text-muted)', marginBottom: '12px' }}>Materialized Tables</h3>
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px' }}>
                  {dbtTables.map(t => (
                    <span key={t} onClick={() => { setQuerySql(`SELECT * FROM ${t} LIMIT 100`); setSection('query'); }} style={{ padding: '6px 14px', borderRadius: '6px', background: 'rgba(16,185,129,0.1)', border: '1px solid rgba(16,185,129,0.3)', color: 'var(--accent-success)', fontSize: '13px', cursor: 'pointer' }}>
                      {t}
                    </span>
                  ))}
                </div>
              </div>
            )}

            {/* Job History */}
            {dbtJobs.length > 0 && (
              <div className="glass-panel" style={{ padding: '16px' }}>
                <h3 style={{ fontSize: '12px', textTransform: 'uppercase', color: 'var(--text-muted)', marginBottom: '12px' }}>Recent Jobs</h3>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                  {dbtJobs.slice(0, 5).map(j => (
                    <div key={j.id} style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '8px 12px', borderRadius: '6px', background: 'rgba(0,0,0,0.15)' }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                        <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: j.status === 'success' ? 'var(--accent-success)' : j.status === 'failed' ? 'var(--accent-danger)' : 'var(--text-muted)' }}></span>
                        <span style={{ fontSize: '13px', fontFamily: 'var(--font-mono)' }}>dbt {j.command}</span>
                      </div>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                        {j.tables?.length > 0 && <span className="text-muted" style={{ fontSize: '11px' }}>{j.tables.length} tables</span>}
                        {j.duration && <span className="text-muted" style={{ fontSize: '11px' }}>{j.duration}s</span>}
                        <span style={{ fontSize: '11px', padding: '2px 8px', borderRadius: '9999px', background: j.status === 'success' ? 'rgba(16,185,129,0.15)' : 'rgba(239,68,68,0.15)', color: j.status === 'success' ? 'var(--accent-success)' : 'var(--accent-danger)' }}>{j.status}</span>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}
        {section === 'support' && (
          <div className="query-editor-wrapper" style={{ maxWidth: '720px', flex: 1, paddingBottom: '40px' }}>
            <h2 style={{ marginBottom: '8px' }}>Support</h2>
            <p className="text-muted" style={{ marginBottom: '28px' }}>Reach out to our team and we'll get back to you as soon as possible.</p>
            <div className="glass-panel" style={{ padding: '28px', display: 'flex', flexDirection: 'column', gap: '16px' }}>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '14px' }}>
                  <input
                    type="text"
                    placeholder="Your name"
                    value={supportForm.name}
                    onChange={e => setSupportForm(f => ({ ...f, name: e.target.value }))}
                    style={{ padding: '10px 14px', borderRadius: '8px', border: '1px solid var(--surface-border)', background: 'var(--surface-glass)', color: 'var(--text-main)', fontSize: '14px', outline: 'none', fontFamily: 'var(--font-body)' }}
                  />
                  <input
                    type="email"
                    placeholder="Your email"
                    value={supportForm.email}
                    onChange={e => setSupportForm(f => ({ ...f, email: e.target.value }))}
                    style={{ padding: '10px 14px', borderRadius: '8px', border: '1px solid var(--surface-border)', background: 'var(--surface-glass)', color: 'var(--text-main)', fontSize: '14px', outline: 'none', fontFamily: 'var(--font-body)' }}
                  />
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                  <label style={{ fontSize: '12px', textTransform: 'uppercase', letterSpacing: '0.05em', color: 'var(--text-muted)' }}>Subject</label>
                  <div style={{ display: 'flex', gap: '0', borderRadius: '9999px', overflow: 'hidden', border: '1px solid var(--surface-border)', alignSelf: 'flex-start' }}>
                    {(['Technical', 'Billing', 'Feedback'] as const).map(s => (
                      <button
                        key={s}
                        onClick={() => setSupportForm(f => ({ ...f, subject: s }))}
                        style={{
                          padding: '8px 20px', border: 'none', cursor: 'pointer',
                          fontSize: '13px', fontFamily: 'var(--font-display)', fontWeight: 600,
                          background: supportForm.subject === s ? 'var(--accent-primary)' : 'rgba(0,0,0,0.2)',
                          color: supportForm.subject === s ? '#fff' : 'var(--text-muted)',
                          transition: 'all 0.2s'
                        }}
                      >{s}</button>
                    ))}
                  </div>
                </div>
                <textarea
                  placeholder="Describe your issue or feedback in detail..."
                  rows={5}
                  value={supportForm.message}
                  onChange={e => setSupportForm(f => ({ ...f, message: e.target.value }))}
                  style={{ padding: '12px 14px', borderRadius: '8px', border: '1px solid var(--surface-border)', background: 'var(--surface-glass)', color: 'var(--text-main)', fontSize: '14px', outline: 'none', resize: 'none', maxHeight: '160px', fontFamily: 'var(--font-body)' }}
                />
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                  {supportStatus && <span style={{ fontSize: '13px', color: 'var(--accent-success)' }}>{supportStatus}</span>}
                  <button
                    className="btn-primary"
                    style={{ marginLeft: 'auto', padding: '10px 28px' }}
                    onClick={async () => {
                      if (!supportForm.name || !supportForm.email || !supportForm.message) {
                        setSupportStatus('Please fill in all fields.');
                        return;
                      }
                      try {
                        setSupportStatus('Sending...');
                        await sendContactEmail({ name: supportForm.name, email: supportForm.email, subject: supportForm.subject, message: supportForm.message });
                        setSupportStatus(`✓ Message sent! We'll be in touch soon.`);
                        addLog(`Support request sent — ${supportForm.subject}`, 'ok');
                        setSupportForm({ name: '', email: '', subject: 'Technical', message: '' });
                      } catch (e: any) { setSupportStatus(`Error: ${e.message}`); }
                    }}
                  >
                    Send Message →
                  </button>
                </div>
            </div>
          </div>
        )}
      </div>

      {/* Global Water / Pond animation */}
      <div className="pond-bg">
        <div className="pond-wave pond-wave-1" />
        <div className="pond-wave pond-wave-2" />
        <div className="pond-wave pond-wave-3" />
      </div>

      {/* Upgrade Plan Modal */}
      {showUpgradeModal && !isLocked && (
        <div style={{ position: 'fixed', top: 0, left: 0, right: 0, bottom: 0, background: 'rgba(0,0,0,0.6)', backdropFilter: 'blur(8px)', zIndex: 1000, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <div className="glass-panel" style={{ width: '800px', maxWidth: '90vw', padding: '40px', display: 'flex', flexDirection: 'column', gap: '32px', position: 'relative' }}>
            <button onClick={() => setShowUpgradeModal(false)} style={{ position: 'absolute', top: '16px', right: '16px', background: 'none', border: 'none', color: 'var(--text-muted)', cursor: 'pointer', fontSize: '20px' }}>×</button>
            <div style={{ textAlign: 'center' }}>
              <h2 style={{ fontSize: '28px', marginBottom: '8px', fontFamily: 'var(--font-display)' }}>Upgrade shikipond</h2>
              <p className="text-muted">Unlock massive analytical power with highly profitable scaling.</p>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '20px' }}>
              <div className="glass-panel" style={{ padding: '24px', background: 'rgba(255,255,255,0.02)' }}>
                <h3 style={{ fontSize: '18px', marginBottom: '4px', color: 'var(--accent-primary)' }}>Starter</h3>
                <div style={{ fontSize: '24px', fontWeight: 'bold', marginBottom: '16px' }}>$7.99<span style={{ fontSize: '14px', color: 'var(--text-muted)', fontWeight: 'normal' }}> / mo</span></div>
                <ul className="text-muted" style={{ fontSize: '13px', paddingLeft: '16px', display: 'flex', flexDirection: 'column', gap: '8px', marginBottom: '24px' }}>
                  <li>50 GB Storage Max</li>
                  <li>$0.028 Compute per 60s</li>
                  <li>7-Day Free Trial</li>
                </ul>
                <button className="btn-secondary" style={{ width: '100%', justifyContent: 'center' }} onClick={() => window.open(`https://buy.stripe.com/fZu14o8xB9lMeS88Q13cc03${session?.user?.id ? '?client_reference_id=' + session.user.id : ''}`, "_blank")}>
                  {billingProfile?.plan_tier === 'starter' && !isTrialExpired ? 'Current Plan' : 'Subscribe ($7.99)'}
                </button>
              </div>
              <div className="glass-panel" style={{ padding: '24px', background: 'rgba(16,185,129,0.05)', border: '1px solid rgba(16,185,129,0.3)' }}>
                <h3 style={{ fontSize: '18px', marginBottom: '4px', color: 'var(--accent-success)' }}>Pro</h3>
                <div style={{ fontSize: '24px', fontWeight: 'bold', marginBottom: '16px' }}>$14.99<span style={{ fontSize: '14px', color: 'var(--text-muted)', fontWeight: 'normal' }}> / mo</span></div>
                <div className="glass-pill" style={{ padding: '4px 10px', fontSize: '10px', display: 'inline-block', marginBottom: '12px', color: '#8B5CF6' }}>100 GB STORAGE</div>
                <ul className="text-muted" style={{ fontSize: '13px', paddingLeft: '16px', display: 'flex', flexDirection: 'column', gap: '8px', marginBottom: '24px' }}>
                  <li>100 GB Storage Max</li>
                  <li>$0.028 Compute per 60s</li>
                  <li>Email Support</li>
                </ul>
                <button className="btn-primary" style={{ width: '100%', justifyContent: 'center' }} onClick={() => window.open(`https://buy.stripe.com/5kQ3cwg031Tk9xO9U53cc04${session?.user?.id ? '?client_reference_id=' + session.user.id : ''}`, "_blank")}>Select Pro</button>
              </div>
              <div className="glass-panel" style={{ padding: '24px', background: 'rgba(0,153,204,0.05)' }}>
                <h3 style={{ fontSize: '18px', marginBottom: '4px', color: 'var(--accent-primary)' }}>Team</h3>
                <div style={{ fontSize: '24px', fontWeight: 'bold', marginBottom: '16px' }}>$99.00<span style={{ fontSize: '14px', color: 'var(--text-muted)', fontWeight: 'normal' }}> / 10 seats</span></div>
                <div className="glass-pill" style={{ padding: '4px 10px', fontSize: '10px', display: 'inline-block', marginBottom: '12px', color: '#F59E0B' }}>1 TB STORAGE</div>
                <ul className="text-muted" style={{ fontSize: '13px', paddingLeft: '16px', display: 'flex', flexDirection: 'column', gap: '8px', marginBottom: '24px' }}>
                  <li>1 TB Storage Pool</li>
                  <li>Discounted $0.025 Compute</li>
                  <li>Priority Phone Support</li>
                </ul>
                <button className="btn-primary" style={{ width: '100%', justifyContent: 'center' }} onClick={() => window.open(`https://buy.stripe.com/14A7sM0150PgaBS7LX3cc05${session?.user?.id ? '?client_reference_id=' + session.user.id : ''}`, "_blank")}>Select Team</button>
              </div>
            </div>
          </div>
        </div>
      )}


      {/* Hardblock Wall: Trial Expired or Out of Credits */}
      {isLocked && (
        <div style={{ position: 'fixed', top: 0, left: 0, right: 0, bottom: 0, background: 'var(--bg-core)', zIndex: 9999, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <div className="glass-panel" style={{ padding: '40px', width: '500px', display: 'flex', flexDirection: 'column', gap: '20px', textAlign: 'center', border: isTrialExpired ? '1px solid var(--accent-primary)' : '1px solid var(--accent-danger)' }}>
            <h1 style={{ fontSize: '28px', color: isOutofCredits ? 'var(--accent-danger)' : 'var(--text-main)', margin: 0 }}>
              {isOutofCredits ? 'Compute Exhausted' : 'Trial Expired'}
            </h1>
            <p className="text-muted" style={{ lineHeight: '1.6' }}>
              {isOutofCredits
                ? 'Your engine has successfully burned through its allocated compute balance handling data requests. To continue using shikipond instantly, please recharge or upgrade your tier.'
                : 'Your 7-Day trial on the Starter tier has successfully concluded. To continue analyzing your Lakehouse, please select a production plan.'}
            </p>
            <div style={{ display: 'flex', gap: '12px', marginTop: '16px' }}>
              <button className="btn-secondary" style={{ flex: 1, justifyContent: 'center', padding: '12px' }} onClick={handleLogout}>Sign Out</button>
              <button className="btn-primary" style={{ flex: 2, justifyContent: 'center', padding: '12px' }} onClick={() => setShowUpgradeModal(true)}>
                {isOutofCredits ? 'Recharge / Upgrade →' : 'Pick a Production Plan →'}
              </button>
            </div>

            {showUpgradeModal && (
              <div style={{ marginTop: '12px' }}>
                <p className="text-muted" style={{ fontSize: '12px' }}>Stripe pricing table will load here...</p>
                <button onClick={() => window.open(`https://buy.stripe.com/5kQ3cwg031Tk9xO9U53cc04${session?.user?.id ? '?client_reference_id=' + session.user.id : ''}`, "_blank")} className="btn-primary" style={{ width: '100%', justifyContent: 'center' }}>Open Checkout (Pro)</button>
              </div>
            )}

          </div>
        </div>
      )}
      <div style={{ position: 'absolute', bottom: '8px', right: '16px', fontSize: '11px', color: 'var(--text-muted)', zIndex: 9999 }}>
        © 2026 shikipond. All rights reserved.
      </div>
    </div>
  );
}
