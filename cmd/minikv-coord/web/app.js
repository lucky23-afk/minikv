function minikvApp() {
  return {
    currentTab: 'overview',
    darkMode: localStorage.getItem('minikv-theme') === 'dark',
    syncing: false,
    tabTitles: {
      overview: 'Cluster Overview',
      cluster: 'Cluster Nodes & Hash Ring',
      explorer: 'Data Explorer',
      console: 'Interactive Console',
      persistence: 'Persistence & Replication',
      benchmarks: 'Performance Benchmarks'
    },
    stats: {
      nodes_total: 0,
      nodes_alive: 0,
      hits: 0,
      misses: 0,
      hit_rate: 0,
      evictions: 0,
      keys: 0,
      bytes: 0
    },
    nodes: [],
    ringVNodes: [],
    lookupKey: '',
    lookupMappedNodes: [],
    keyList: [],
    keyPrefix: '',
    consoleInput: '',
    consoleLines: [],
    showKeyModal: false,
    modalKey: { key: '', val: '', ttl: 0 },
    benchForm: {
      concurrency: 10,
      duration_sec: 5,
      read_ratio: 0.8
    },
    benchRunning: false,
    benchHistory: [],
    metricPoints: [],

    initApp() {
      if (this.darkMode) {
        document.documentElement.classList.add('dark');
      }
      this.refreshAll();
      this.startSSE();
      this.initChartLoop();
    },

    toggleDarkMode() {
      this.darkMode = !this.darkMode;
      localStorage.setItem('minikv-theme', this.darkMode ? 'dark' : 'light');
      if (this.darkMode) {
        document.documentElement.classList.add('dark');
      } else {
        document.documentElement.classList.remove('dark');
      }
    },

    async refreshAll() {
      await Promise.all([
        this.fetchStats(),
        this.fetchNodes(),
        this.fetchRing(),
        this.fetchKeys(),
        this.fetchBenchHistory()
      ]);
    },

    startSSE() {
      const evtSource = new EventSource('/api/v1/events');
      evtSource.onmessage = (e) => {
        this.syncing = true;
        setTimeout(() => this.syncing = false, 300);
        this.fetchStats();
        this.fetchNodes();
      };
      evtSource.onerror = () => {
        // Fallback polling if SSE disconnects
        setTimeout(() => this.fetchStats(), 2000);
      };
    },

    async fetchStats() {
      try {
        const res = await fetch('/api/v1/stats');
        if (res.ok) {
          this.stats = await res.json();
          this.pushMetricPoint(this.stats.keys);
        }
      } catch (e) {}
    },

    async fetchNodes() {
      try {
        const res = await fetch('/api/v1/nodes');
        if (res.ok) {
          this.nodes = await res.json();
        }
      } catch (e) {}
    },

    async removeNode(id) {
      if (!confirm(`Are you sure you want to decommission node ${id}?`)) return;
      await fetch(`/api/v1/nodes/${id}`, { method: 'DELETE' });
      await this.refreshAll();
    },

    async fetchRing() {
      try {
        const url = this.lookupKey ? `/api/v1/ring?key=${encodeURIComponent(this.lookupKey)}` : '/api/v1/ring';
        const res = await fetch(url);
        if (res.ok) {
          const data = await res.json();
          this.ringVNodes = data.vnodes || [];
          this.lookupMappedNodes = data.mapped_nodes || [];
        }
      } catch (e) {}
    },

    updateRingLookup() {
      this.fetchRing();
    },

    getCoord(hashVal, radius) {
      // Normalize uint64 to angle 0..2PI
      const angle = (Number(BigInt(hashVal) % BigInt(3600)) / 3600.0) * 2 * Math.PI;
      return {
        x: 150 + radius * Math.cos(angle),
        y: 150 + radius * Math.sin(angle)
      };
    },

    getNodeColor(nodeID) {
      const colors = ['#1D4ED8', '#0D9488', '#D97706', '#7C3AED', '#DB2777', '#059669'];
      let hash = 0;
      for (let i = 0; i < nodeID.length; i++) hash += nodeID.charCodeAt(i);
      return colors[hash % colors.length];
    },

    async fetchKeys() {
      try {
        const url = this.keyPrefix ? `/api/v1/kv?prefix=${encodeURIComponent(this.keyPrefix)}` : '/api/v1/kv';
        const res = await fetch(url);
        if (res.ok) {
          this.keyList = await res.json() || [];
        }
      } catch (e) {}
    },

    openKeyModal() {
      this.modalKey = { key: '', val: '', ttl: 0 };
      this.showKeyModal = true;
    },

    async saveKey() {
      await fetch(`/api/v1/kv/${encodeURIComponent(this.modalKey.key)}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          value: this.modalKey.val,
          ttl_ms: this.modalKey.ttl
        })
      });
      this.showKeyModal = false;
      await this.fetchKeys();
      await this.fetchStats();
    },

    async deleteKey(key) {
      if (!confirm(`Delete key ${key}?`)) return;
      await fetch(`/api/v1/kv/${encodeURIComponent(key)}`, { method: 'DELETE' });
      await this.fetchKeys();
      await this.fetchStats();
    },

    async runConsoleCommand() {
      const cmd = this.consoleInput.trim();
      if (!cmd) return;
      this.consoleInput = '';

      try {
        const res = await fetch('/api/v1/console', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ cmd })
        });
        const data = await res.json();
        this.consoleLines.push({ cmd, res: data.output || data.error });
      } catch (err) {
        this.consoleLines.push({ cmd, res: `(error) ${err.message}` });
      }

      this.$nextTick(() => {
        const el = document.getElementById('consoleOutput');
        if (el) el.scrollTop = el.scrollHeight;
      });
    },

    async triggerSnapshot() {
      const res = await fetch('/api/v1/snapshot', { method: 'POST' });
      const data = await res.json();
      alert(data.message || 'Snapshot triggered');
    },

    async fetchBenchHistory() {
      try {
        const res = await fetch('/api/v1/bench/history');
        if (res.ok) {
          this.benchHistory = await res.json();
        }
      } catch (e) {}
    },

    async runBenchmark() {
      this.benchRunning = true;
      try {
        const res = await fetch('/api/v1/bench/run', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(this.benchForm)
        });
        if (res.ok) {
          const run = await res.json();
          this.benchHistory.unshift(run);
        }
      } finally {
        this.benchRunning = false;
      }
    },

    formatBytes(bytes) {
      if (!bytes || bytes === 0) return '0 B';
      const k = 1024;
      const sizes = ['B', 'KB', 'MB', 'GB'];
      const i = Math.floor(Math.log(bytes) / Math.log(k));
      return (bytes / Math.pow(k, i)).toFixed(1) + ' ' + sizes[i];
    },

    pushMetricPoint(val) {
      this.metricPoints.push(val);
      if (this.metricPoints.length > 40) this.metricPoints.shift();
      this.drawChart();
    },

    initChartLoop() {
      setTimeout(() => this.drawChart(), 200);
    },

    drawChart() {
      const canvas = document.getElementById('metricsChart');
      if (!canvas) return;
      const ctx = canvas.getContext('2d');
      const w = canvas.width;
      const h = canvas.height;

      ctx.clearRect(0, 0, w, h);

      // Draw subtle grid lines
      ctx.strokeStyle = this.darkMode ? '#1F2937' : '#E5E7EB';
      ctx.lineWidth = 1;
      for (let y = 20; y < h; y += 40) {
        ctx.beginPath();
        ctx.moveTo(0, y);
        ctx.lineTo(w, y);
        ctx.stroke();
      }

      if (this.metricPoints.length < 2) return;

      const maxVal = Math.max(...this.metricPoints, 10);
      const step = w / (this.metricPoints.length - 1);

      ctx.strokeStyle = '#1D4ED8';
      ctx.lineWidth = 2;
      ctx.beginPath();

      this.metricPoints.forEach((val, i) => {
        const x = i * step;
        const y = h - (val / maxVal) * (h - 40) - 20;
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      });
      ctx.stroke();
    }
  };
}
