package api

// DashboardHTML contains the embedded, self-contained single-page dashboard.
const DashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>GoQueue — Distributed Asynchronous Task Engine</title>
  <style>
    :root {
      --bg: #0b0f19;
      --card-bg: #111827;
      --card-border: #1f2937;
      --text: #f9fafb;
      --text-muted: #9ca3af;
      --accent: #3b82f6;
      --accent-hover: #2563eb;
      --crit: #ef4444;
      --high: #f59e0b;
      --def: #3b82f6;
      --low: #10b981;
      --success: #10b981;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }
    body { background-color: var(--bg); color: var(--text); padding: 24px; min-height: 100vh; }
    header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px; padding-bottom: 16px; border-bottom: 1px solid var(--card-border); }
    .brand { display: flex; align-items: center; gap: 12px; }
    .logo-badge { background: linear-gradient(135deg, #3b82f6, #8b5cf6); padding: 8px 14px; border-radius: 8px; font-weight: 800; font-size: 1.1rem; letter-spacing: 1px; }
    .status-dot { width: 10px; height: 10px; border-radius: 50%; background-color: var(--success); display: inline-block; margin-right: 6px; box-shadow: 0 0 8px var(--success); }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 24px; }
    .card { background-color: var(--card-bg); border: 1px solid var(--card-border); border-radius: 10px; padding: 18px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.3); }
    .card-title { font-size: 0.8rem; color: var(--text-muted); text-transform: uppercase; font-weight: 600; letter-spacing: 0.5px; margin-bottom: 8px; }
    .card-val { font-size: 1.8rem; font-weight: 700; color: #fff; }
    .section-title { font-size: 1.25rem; font-weight: 600; margin: 28px 0 16px 0; display: flex; justify-content: space-between; align-items: center; }
    .btn { background-color: var(--accent); color: white; border: none; padding: 8px 16px; border-radius: 6px; cursor: pointer; font-size: 0.875rem; font-weight: 600; transition: background-color 0.2s; display: inline-flex; align-items: center; gap: 6px; }
    .btn:hover { background-color: var(--accent-hover); }
    .btn-danger { background-color: #dc2626; }
    .btn-danger:hover { background-color: #b91c1c; }
    .btn-secondary { background-color: #374151; }
    .btn-secondary:hover { background-color: #4b5563; }
    .btn-sm { padding: 4px 10px; font-size: 0.75rem; border-radius: 4px; }
    table { width: 100%; border-collapse: collapse; margin-top: 8px; background-color: var(--card-bg); border-radius: 8px; overflow: hidden; border: 1px solid var(--card-border); }
    th, td { padding: 12px 16px; text-align: left; border-bottom: 1px solid var(--card-border); font-size: 0.875rem; }
    th { background-color: #1a2234; color: var(--text-muted); font-weight: 600; text-transform: uppercase; font-size: 0.75rem; }
    tr:last-child td { border-bottom: none; }
    tr:hover { background-color: rgba(255,255,255,0.02); }
    .badge { display: inline-block; padding: 3px 8px; border-radius: 4px; font-size: 0.75rem; font-weight: 600; text-transform: uppercase; }
    .badge-critical { background-color: rgba(239, 68, 68, 0.2); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.4); }
    .badge-high { background-color: rgba(245, 158, 11, 0.2); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.4); }
    .badge-default { background-color: rgba(59, 130, 246, 0.2); color: #60a5fa; border: 1px solid rgba(59, 130, 246, 0.4); }
    .badge-low { background-color: rgba(16, 185, 129, 0.2); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.4); }
    .badge-paused { background-color: rgba(239, 68, 68, 0.2); color: #f87171; }
    .badge-active { background-color: rgba(16, 185, 129, 0.2); color: #34d399; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 0.8rem; }
    .modal { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.7); backdrop-filter: blur(4px); justify-content: center; align-items: center; z-index: 100; }
    .modal-content { background: var(--card-bg); border: 1px solid var(--card-border); padding: 24px; border-radius: 12px; width: 650px; max-width: 90%; max-height: 85vh; overflow-y: auto; }
    pre { background: #000; padding: 12px; border-radius: 6px; overflow-x: auto; font-family: monospace; font-size: 0.8rem; color: #a7f3d0; margin-top: 10px; }
    .controls { display: flex; gap: 8px; }
    .empty-state { padding: 32px; text-align: center; color: var(--text-muted); font-size: 0.9rem; }
  </style>
</head>
<body>
  <header>
    <div class="brand">
      <div class="logo-badge">GQ</div>
      <div>
        <h2>GoQueue Engine</h2>
        <p style="font-size: 0.8rem; color: var(--text-muted);"><span class="status-dot"></span>Active Cluster &bull; Distributed Redis Backend</p>
      </div>
    </div>
    <div class="controls">
      <a href="/metrics" target="_blank" class="btn btn-secondary btn-sm">Prometheus /metrics</a>
      <button class="btn btn-sm" onclick="triggerLoadTest()">&#9889; Generate Load</button>
      <button class="btn btn-secondary btn-sm" onclick="fetchData()">&#8635; Refresh</button>
    </div>
  </header>

  <!-- Metric Overview Cards -->
  <div class="grid">
    <div class="card">
      <div class="card-title">Active Workers</div>
      <div class="card-val" id="metric-workers">--</div>
    </div>
    <div class="card">
      <div class="card-title">In-Flight Tasks</div>
      <div class="card-val" id="metric-inflight" style="color: #60a5fa;">--</div>
    </div>
    <div class="card">
      <div class="card-title">Ready Tasks</div>
      <div class="card-val" id="metric-ready" style="color: #34d399;">--</div>
    </div>
    <div class="card">
      <div class="card-title">Active Leases</div>
      <div class="card-val" id="metric-active" style="color: #fbbf24;">--</div>
    </div>
    <div class="card">
      <div class="card-title">Scheduled / Delayed</div>
      <div class="card-val" id="metric-scheduled" style="color: #a78bfa;">--</div>
    </div>
    <div class="card">
      <div class="card-title">Dead-Letter Queue (DLQ)</div>
      <div class="card-val" id="metric-dlq" style="color: #f87171;">--</div>
    </div>
    <div class="card">
      <div class="card-title">Total Processed</div>
      <div class="card-val" id="metric-processed" style="color: #34d399;">--</div>
    </div>
  </div>

  <!-- Queues Table -->
  <div class="section-title">
    <span>Queues & Multi-tier Priorities</span>
  </div>
  <table>
    <thead>
      <tr>
        <th>Queue Name</th>
        <th>Status</th>
        <th>Critical</th>
        <th>High</th>
        <th>Default</th>
        <th>Low</th>
        <th>Active Leases</th>
        <th>Scheduled</th>
        <th>DLQ</th>
        <th>Actions</th>
      </tr>
    </thead>
    <tbody id="queues-tbody">
      <tr><td colspan="10" class="empty-state">Loading queue state...</td></tr>
    </tbody>
  </table>

  <!-- Dead-Letter Queue (DLQ) -->
  <div class="section-title">
    <span>Dead-Letter Queue (Failed Jobs)</span>
    <div class="controls">
      <button class="btn btn-secondary btn-sm" onclick="retryAllDLQ()">Retry All</button>
      <button class="btn btn-danger btn-sm" onclick="purgeDLQ()">Purge DLQ</button>
    </div>
  </div>
  <table>
    <thead>
      <tr>
        <th>Task ID</th>
        <th>Type</th>
        <th>Queue</th>
        <th>Priority</th>
        <th>Retries</th>
        <th>Last Error</th>
        <th>Actions</th>
      </tr>
    </thead>
    <tbody id="dlq-tbody">
      <tr><td colspan="7" class="empty-state">No failed tasks in Dead-Letter Queue. System healthy!</td></tr>
    </tbody>
  </table>

  <!-- Modal for Error Details -->
  <div id="stack-modal" class="modal" onclick="closeModal(event)">
    <div class="modal-content">
      <div style="display:flex; justify-content:space-between; align-items:center;">
        <h3 id="modal-title">Error & Stack Trace</h3>
        <button class="btn btn-secondary btn-sm" onclick="document.getElementById('stack-modal').style.display='none'">&#10005;</button>
      </div>
      <p id="modal-error" style="color: #f87171; margin-top: 12px; font-weight: 600;"></p>
      <pre id="modal-stack"></pre>
    </div>
  </div>

  <script>
    let globalData = null;

    async function fetchData() {
      try {
        const res = await fetch('/api/v1/stats');
        const data = await res.json();
        globalData = data;
        renderDashboard(data);
        fetchDLQ();
      } catch (e) {
        console.error('Fetch error:', e);
      }
    }

    function renderDashboard(data) {
      document.getElementById('metric-workers').innerText = data.active_workers || '0';
      document.getElementById('metric-inflight').innerText = data.in_flight_tasks || '0';

      let totalReady = 0, totalActive = 0, totalSched = 0, totalDlq = 0, totalProcessed = 0;
      const qTbody = document.getElementById('queues-tbody');
      qTbody.innerHTML = '';

      const queues = data.queues || {};
      for (const [qName, q] of Object.entries(queues)) {
        totalReady += q.total_ready || 0;
        totalActive += q.active || 0;
        totalSched += q.scheduled || 0;
        totalDlq += q.dead_letter || 0;
        totalProcessed += q.total_processed || 0;

        const isPaused = q.is_paused;
        const statusBadge = isPaused
          ? '<span class="badge badge-paused">PAUSED</span>'
          : '<span class="badge badge-active">RUNNING</span>';

        const pauseBtn = isPaused
          ? '<button class="btn btn-sm btn-secondary" onclick="resumeQueue(\'' + qName + '\')">Resume</button>'
          : '<button class="btn btn-sm btn-secondary" onclick="pauseQueue(\'' + qName + '\')">Pause</button>';

        const crit = (q.ready_by_priority && q.ready_by_priority.critical) || 0;
        const high = (q.ready_by_priority && q.ready_by_priority.high) || 0;
        const def = (q.ready_by_priority && q.ready_by_priority.default) || 0;
        const low = (q.ready_by_priority && q.ready_by_priority.low) || 0;

        const row = document.createElement('tr');
        row.innerHTML = '<td><strong>' + qName + '</strong></td>' +
          '<td>' + statusBadge + '</td>' +
          '<td><span class="badge badge-critical">' + crit + '</span></td>' +
          '<td><span class="badge badge-high">' + high + '</span></td>' +
          '<td><span class="badge badge-default">' + def + '</span></td>' +
          '<td><span class="badge badge-low">' + low + '</span></td>' +
          '<td>' + (q.active || 0) + '</td>' +
          '<td>' + (q.scheduled || 0) + '</td>' +
          '<td><span style="color: ' + (q.dead_letter > 0 ? '#f87171' : 'inherit') + '">' + (q.dead_letter || 0) + '</span></td>' +
          '<td>' + pauseBtn + '</td>';
        qTbody.appendChild(row);
      }

      document.getElementById('metric-ready').innerText = totalReady;
      document.getElementById('metric-active').innerText = totalActive;
      document.getElementById('metric-scheduled').innerText = totalSched;
      document.getElementById('metric-dlq').innerText = totalDlq;
      document.getElementById('metric-processed').innerText = totalProcessed;
    }

    async function fetchDLQ() {
      try {
        const res = await fetch('/api/v1/dlq?limit=20');
        const data = await res.json();
        const tbody = document.getElementById('dlq-tbody');
        tbody.innerHTML = '';

        if (!data.tasks || data.tasks.length === 0) {
          tbody.innerHTML = '<tr><td colspan="7" class="empty-state">No failed tasks in Dead-Letter Queue. System healthy!</td></tr>';
          return;
        }

        data.tasks.forEach(t => {
          const row = document.createElement('tr');
          const shortID = t.id.substring(0, 8);
          row.innerHTML = '<td class="mono" title="' + t.id + '">' + shortID + '...</td>' +
            '<td><strong>' + t.type + '</strong></td>' +
            '<td>' + t.queue + '</td>' +
            '<td><span class="badge badge-' + t.priority + '">' + t.priority + '</span></td>' +
            '<td>' + t.current_retry + ' / ' + t.max_retries + '</td>' +
            '<td style="color: #f87171; max-width: 250px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">' + (t.last_error || 'Unknown error') + '</td>' +
            '<td>' +
              '<button class="btn btn-secondary btn-sm" onclick=\'showTrace(' + JSON.stringify(t) + ')\'>Inspect</button> ' +
              '<button class="btn btn-sm" onclick="retryTask(\'' + t.id + '\', \'' + t.queue + '\')">Retry</button>' +
            '</td>';
          tbody.appendChild(row);
        });
      } catch (e) {
        console.error('DLQ fetch error:', e);
      }
    }

    function showTrace(t) {
      document.getElementById('modal-title').innerText = "Task " + t.id;
      document.getElementById('modal-error').innerText = t.last_error || 'No error message';
      document.getElementById('modal-stack').innerText = t.stack_trace || 'No stack trace captured.';
      document.getElementById('stack-modal').style.display = 'flex';
    }

    function closeModal(e) {
      if (e.target.id === 'stack-modal') {
        document.getElementById('stack-modal').style.display = 'none';
      }
    }

    async function retryTask(taskID, queue) {
      await fetch('/api/v1/dlq/' + taskID + '/retry?queue=' + queue, { method: 'POST' });
      fetchData();
    }

    async function retryAllDLQ() {
      await fetch('/api/v1/dlq/retry-all', { method: 'POST' });
      fetchData();
    }

    async function purgeDLQ() {
      if (confirm('Are you sure you want to purge the Dead-Letter Queue?')) {
        await fetch('/api/v1/dlq', { method: 'DELETE' });
        fetchData();
      }
    }

    async function pauseQueue(q) {
      await fetch('/api/v1/queues/' + q + '/pause', { method: 'POST' });
      fetchData();
    }

    async function resumeQueue(q) {
      await fetch('/api/v1/queues/' + q + '/resume', { method: 'POST' });
      fetchData();
    }

    async function triggerLoadTest() {
      await fetch('/api/v1/loadgen', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ count: 100, failure_rate: 0.1, delay_rate: 0.2 })
      });
      fetchData();
    }

    // Auto poll every 2 seconds
    fetchData();
    setInterval(fetchData, 2000);
  </script>
</body>
</html>`
