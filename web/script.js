/**
 * script.js — AIoT Watch Dashboard
 * MQTT WebSocket: broker.emqx.io:8083/mqtt
 *
 * MODE OTOMATIS:
 *   DATA COLLECTION MODE — hanya sensor/esp32/data (server_knn TIDAK perlu)
 *                          Tampil: nilai sensor live, BPM, grafik real-time
 *   KNN MODE             — classification/result diterima (server_knn aktif)
 *                          Tampil: aktivitas + confidence + bar chart
 *
 * Deteksi otomatis: classification/result masuk → KNN mode.
 * 10 detik tanpa classification → kembali ke DATA COLLECTION mode.
 */

'use strict';

const CONFIG = {
  broker: 'broker.emqx.io',
  port: 8083,
  path: '/mqtt',
  clientId: 'dashboard_' + Math.random().toString(16).slice(2, 10),
  topics: {
    sensorData:     'sensor/esp32/data',
    classification: 'classification/result',
  },
};

const MAX_POINTS = 200;
const sensorBuffer = { labels: [], accel: [], gyro: [], bpm: [] };
let sensorChart   = null;
let activityChart = null;
let msgCount      = 0;
const activityHistory = [];

// ── Mode tracking ─────────────────────────────────────────────────────────────
let knnMode          = false;
let lastKnnTimestamp = 0;
const KNN_TIMEOUT_MS = 10000;

// Cek periodis apakah KNN masih kirim data
setInterval(() => {
  if (knnMode && Date.now() - lastKnnTimestamp > KNN_TIMEOUT_MS) {
    setKnnMode(false);
  }
}, 2000);

function setKnnMode(active) {
  if (knnMode === active) return;
  knnMode = active;
  renderModeUI();
}

function renderModeUI() {
  const modeBadge      = document.getElementById('modeBadge');
  const modeLabel      = document.getElementById('modeLabel');
  const knnPanel       = document.getElementById('knnPanel');
  const sensorPanel    = document.getElementById('sensorPanel');
  const actHeader      = document.getElementById('actChartHeader');
  const actPlaceholder = document.getElementById('actPlaceholder');
  const actChartWrap   = document.getElementById('actChartWrap');

  if (knnMode) {
    modeBadge.className   = 'mode-badge knn-active';
    modeLabel.textContent = '● KNN AKTIF';
    knnPanel.style.display       = 'block';
    sensorPanel.style.display    = 'none';
    if (actHeader)      actHeader.textContent        = '📊 ACTIVITY RESULT (Last Hour)';
    if (actPlaceholder) actPlaceholder.style.display = 'none';
    if (actChartWrap)   actChartWrap.style.display   = 'block';
  } else {
    modeBadge.className   = 'mode-badge collection';
    modeLabel.textContent = '● COLLECT PARTICIPANT';
    knnPanel.style.display       = 'none';
    sensorPanel.style.display    = 'block';
    if (actHeader)      actHeader.textContent        = '📊 ACTIVITY RESULT';
    if (actPlaceholder) actPlaceholder.style.display = 'flex';
    if (actChartWrap)   actChartWrap.style.display   = 'none';
  }
}

// ── Sensor live card ──────────────────────────────────────────────────────────
function updateSensorCard(accel, gyro, bpm) {
  const elA = document.getElementById('liveAccel');
  const elG = document.getElementById('liveGyro');
  const elB = document.getElementById('liveBpm');
  if (elA) elA.textContent = accel.toFixed(4);
  if (elG) elG.textContent = gyro.toFixed(2);
  if (elB) {
    elB.textContent = bpm > 0 ? bpm : '--';
    elB.style.color = bpm > 0
      ? (bpm >= 140 ? '#F87171' : bpm >= 100 ? '#FBBF24' : '#34D399')
      : 'var(--text-secondary)';
  }
}

// ============================================================
// CHARTS
// ============================================================
function initSensorChart() {
  const canvas = document.getElementById('sensorChart');
  const w = canvas.parentElement.clientWidth;
  canvas.width = w * 2; canvas.height = 320 * 2;
  canvas.style.width = w + 'px'; canvas.style.height = '320px';
  const ctx = canvas.getContext('2d');
  ctx.scale(2, 2);
  sensorChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: sensorBuffer.labels,
      datasets: [
        { label: 'Accel (g)',     data: sensorBuffer.accel, borderColor: '#60A5FA', backgroundColor: 'transparent', borderWidth: 2.5, pointRadius: 0, tension: 0.1, fill: false },
        { label: 'Gyro (°/s÷10)',data: sensorBuffer.gyro,  borderColor: '#34D399', backgroundColor: 'transparent', borderWidth: 2.5, pointRadius: 0, tension: 0.1, fill: false },
        { label: 'BPM÷100',      data: sensorBuffer.bpm,   borderColor: '#F87171', backgroundColor: 'transparent', borderWidth: 2.5, pointRadius: 0, tension: 0.1, fill: false },
      ],
    },
    options: {
      responsive: false, maintainAspectRatio: false, animation: false,
      interaction: { mode: 'index', intersect: false },
      plugins: { legend: { display: false }, tooltip: { backgroundColor: 'rgba(0,0,0,0.85)', titleColor: '#fff', bodyColor: '#ddd', padding: 10, cornerRadius: 8 } },
      scales: {
        x: { grid: { color: 'rgba(128,128,128,0.08)', lineWidth: 0.5 }, ticks: { color: '#9CA3AF', font: { size: 10, family: 'JetBrains Mono' }, maxTicksLimit: 8 } },
        y: { grid: { color: 'rgba(128,128,128,0.08)', lineWidth: 0.5 }, ticks: { color: '#9CA3AF', font: { size: 10, family: 'JetBrains Mono' }, callback: v => v.toFixed(2) }, suggestedMin: 0, suggestedMax: 1.5 },
      },
    },
  });
}

function initActivityChart() {
  const canvas = document.getElementById('activityChart');
  const w = canvas.parentElement.clientWidth;
  canvas.width = w * 2; canvas.height = 210 * 2;
  canvas.style.width = w + 'px'; canvas.style.height = '210px';
  const ctx = canvas.getContext('2d');
  ctx.scale(2, 2);
  activityChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: ['Duduk', 'Berjalan', 'Berlari'],
      datasets: [{ data: [0, 0, 0], backgroundColor: ['rgba(96,165,250,0.85)', 'rgba(52,211,153,0.85)', 'rgba(248,113,113,0.85)'], borderColor: ['#60A5FA', '#34D399', '#F87171'], borderWidth: 1.5, borderRadius: 8, barPercentage: 0.65, categoryPercentage: 0.8 }],
    },
    options: {
      responsive: false, maintainAspectRatio: false,
      animation: { duration: 300, easing: 'easeOutQuad' },
      plugins: { legend: { display: false }, tooltip: { backgroundColor: 'rgba(0,0,0,0.85)', callbacks: { label: c => `${c.raw} kali` } } },
      scales: {
        x: { grid: { display: false }, ticks: { color: '#9CA3AF', font: { size: 12, weight: '600', family: 'Inter' } } },
        y: { grid: { color: 'rgba(128,128,128,0.08)', lineWidth: 0.5 }, ticks: { color: '#9CA3AF', font: { size: 10, family: 'JetBrains Mono' }, stepSize: 1, precision: 0 }, beginAtZero: true },
      },
    },
  });
}

function pushSensorData(accel, gyro, bpm) {
  msgCount++;
  sensorBuffer.labels.push(msgCount);
  sensorBuffer.accel.push(accel);
  sensorBuffer.gyro.push(gyro / 10);
  sensorBuffer.bpm.push(bpm > 0 ? bpm / 100 : null);
  if (sensorBuffer.labels.length > MAX_POINTS) {
    sensorBuffer.labels.shift(); sensorBuffer.accel.shift();
    sensorBuffer.gyro.shift();   sensorBuffer.bpm.shift();
  }
  if (sensorChart) sensorChart.update('none');
}

function pushActivityResult(activity) {
  const now = Date.now();
  activityHistory.push({ time: now, activity });
  while (activityHistory.length > 0 && activityHistory[0].time < now - 3600000) activityHistory.shift();
  const c = { DUDUK: 0, BERJALAN: 0, BERLARI: 0 };
  activityHistory.forEach(r => { if (c[r.activity] !== undefined) c[r.activity]++; });
  if (activityChart) { activityChart.data.datasets[0].data = [c.DUDUK, c.BERJALAN, c.BERLARI]; activityChart.update(); }
}

// ============================================================
// UI UPDATES
// ============================================================
const ACTIVITY_META = {
  DUDUK:    { label: 'DUDUK',    icon: '🪑', sub: 'Sedang duduk / diam',  color: '#60A5FA' },
  BERJALAN: { label: 'BERJALAN', icon: '🚶', sub: 'Sedang berjalan kaki', color: '#34D399' },
  BERLARI:  { label: 'BERLARI',  icon: '🏃', sub: 'Sedang berlari',       color: '#F87171' },
};
let lastActivity = '';

function updateActivity(activity) {
  if (activity === lastActivity) return;
  lastActivity = activity;
  const meta = ACTIVITY_META[activity] || { label: activity, icon: '❓', sub: '', color: '#9CA3AF' };
  const v = document.getElementById('activityValue');
  const i = document.getElementById('actIcon');
  const s = document.getElementById('actSub');
  if (v) v.textContent = meta.label;
  if (i) i.textContent = meta.icon;
  if (s) s.textContent = meta.sub;
}

function updateBPM(bpm) {
  const el = document.getElementById('bpmValue');
  const st = document.getElementById('bpmStatus');
  const bar = document.getElementById('bpmBar');
  if (bpm <= 0) {
    if (el) el.textContent = '--'; if (st) st.textContent = 'Tidak terbaca'; if (bar) bar.style.width = '0%'; return;
  }
  if (el) el.textContent = bpm;
  let label = 'Normal';
  if (bpm < 60) label = 'Terlalu Lambat'; else if (bpm >= 140) label = 'Sangat Tinggi'; else if (bpm >= 100) label = 'Tinggi';
  if (st) st.textContent = label;
  if (bar) bar.style.width = Math.min(100, (bpm / 200) * 100) + '%';
}

function updateAccuracy(confidence) {
  const pct = Math.round(confidence * 100);
  const ring = document.getElementById('ringFill');
  const circ = 2 * Math.PI * 32;
  if (ring) {
    ring.style.strokeDasharray  = circ;
    ring.style.strokeDashoffset = circ - (pct / 100) * circ;
    ring.style.stroke = pct >= 85 ? '#34D399' : pct >= 65 ? '#FBBF24' : '#F87171';
  }
  const el = document.getElementById('accuracyValue');
  if (el) el.textContent = pct + '%';
}

function updateInfo(data) {
  if (data.device_id)                   document.getElementById('infoDevice').textContent   = data.device_id;
  if (data.participant_id || data.user) document.getElementById('infoUser').textContent     = data.participant_id || data.user;
  document.getElementById('infoLastMsg').textContent = new Date().toLocaleTimeString('id-ID');
  document.getElementById('infoTotal').textContent   = msgCount;
  const elSesi = document.getElementById('infoSesi');
  if (elSesi && (data.participant_id || data.user)) {
    const pid = data.participant_id || data.user;
    const pno = data.participant_no ? `P${data.participant_no} — ` : '';
    elSesi.textContent = `${pno}${pid}`;
  }
}

function setConnectionStatus(online) {
  const dot = document.getElementById('badgeDot'), badge = document.getElementById('badgeLabel');
  const sd  = document.getElementById('statusDot'), st    = document.getElementById('statusText');
  if (online) {
    if (dot) dot.className = 'badge-dot online'; if (badge) badge.textContent = 'Connected';
    if (sd)  sd.className  = 'status-dot online'; if (st) st.textContent = 'Connected';
  } else {
    if (dot) dot.className = 'badge-dot'; if (badge) badge.textContent = 'Disconnected';
    if (sd)  sd.className  = 'status-dot'; if (st) st.textContent = 'Disconnected';
  }
}

// ============================================================
// MQTT
// ============================================================
function connectMQTT() {
  const client = mqtt.connect(`ws://${CONFIG.broker}:${CONFIG.port}${CONFIG.path}`, {
    clientId: CONFIG.clientId, connectTimeout: 10000, reconnectPeriod: 5000,
  });

  client.on('connect', () => {
    setConnectionStatus(true);
    Object.values(CONFIG.topics).forEach(t => client.subscribe(t));
  });
  client.on('close', () => setConnectionStatus(false));
  client.on('error', err => console.error('[MQTT]', err.message));

  client.on('message', (topic, payload) => {
    let data;
    try { data = JSON.parse(payload.toString()); } catch (e) { return; }

    if (topic === CONFIG.topics.sensorData) {
      const accel = data.accel_stddev || 0;
      const gyro  = data.gyro_stddev  || 0;
      const bpm   = data.bpm          || 0;
      pushSensorData(accel, gyro, bpm);
      updateSensorCard(accel, gyro, bpm);
      updateBPM(bpm);
      updateInfo(data);
    }
    else if (topic === CONFIG.topics.classification) {
      lastKnnTimestamp = Date.now();
      setKnnMode(true);
      const act = (data.activity || '').toUpperCase();
      updateActivity(act);
      pushActivityResult(act);
      updateAccuracy(data.confidence || 0);
      if (data.bpm > 0) updateBPM(data.bpm);
    }
  });
}

let resizeTimeout;
window.addEventListener('resize', () => {
  clearTimeout(resizeTimeout);
  resizeTimeout = setTimeout(() => {
    if (sensorChart)   { sensorChart.destroy();  initSensorChart();   }
    if (activityChart) { activityChart.destroy(); initActivityChart(); }
  }, 250);
});

// ============================================================
// INIT
// ============================================================
document.addEventListener('DOMContentLoaded', () => {
  initSensorChart();
  initActivityChart();
  document.getElementById('infobroker').textContent = `${CONFIG.broker}:${CONFIG.port}`;
  renderModeUI();
  connectMQTT();
});