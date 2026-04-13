/**
 * script.js — AIoT Watch Dashboard
 * MQTT WebSocket: broker.emqx.io:8083/mqtt
 */

'use strict';

const CONFIG = {
  broker: 'broker.emqx.io',
  port: 8083,
  path: '/mqtt',
  clientId: 'dashboard_' + Math.random().toString(16).slice(2, 10),
  topics: {
    sensorData: 'sensor/esp32/data',
    classification: 'classification/result',
    status: 'status/esp32',
  },
};

const STORAGE_KEY = 'aiot_history_cloud_v2';
const MAX_POINTS = 300;

const sensorBuffer = { labels: [], accel: [], gyro: [], bpm: [] };
let sensorChart = null;
let activityChart = null;
let msgCount = 0;
const activityHistory = [];
let historyRecords = [];

// ============================================================
// HISTORY MANAGEMENT
// ============================================================
function loadHistoryFromStorage() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return raw ? JSON.parse(raw) : [];
  } catch (e) {
    return [];
  }
}

function saveHistoryToStorage(records) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(records));
}

historyRecords = loadHistoryFromStorage();

function addHistoryRecord(data) {
  const no = parseInt(data.participant_no) || (historyRecords.length + 1);
  const pid = data.participant_id || data.user || '—';
  const activity = (data.final_activity || '').toUpperCase();
  const bpm = parseInt(data.final_bpm) || 0;
  const cDuduk = parseInt(data.count_duduk) || 0;
  const cBerjalan = parseInt(data.count_berjalan) || 0;
  const cBerlari = parseInt(data.count_berlari) || 0;
  const totalSamp = parseInt(data.total_samples) || (cDuduk + cBerjalan + cBerlari);
  const waktu = new Date().toLocaleTimeString('id-ID');

  if (historyRecords.some(r => r.no === no && r.pid === pid)) return;

  historyRecords.unshift({ no, pid, activity, bpm, cDuduk, cBerjalan, cBerlari, totalSamp, waktu });
  saveHistoryToStorage(historyRecords);
  renderHistory(true);
  updateHistoryStats();
}

function renderHistory(animateNew = false) {
  const tbody = document.getElementById('historyBody');
  if (!tbody) return;

  if (historyRecords.length === 0) {
    tbody.innerHTML = '<tr class="history-empty"><td colspan="9">Menunggu hasil sesi peserta pertama...</td></tr>';
    return;
  }

  tbody.innerHTML = historyRecords.map((r, i) => `
    <tr class="${animateNew && i === 0 ? 'new-row' : ''}">
      <td>${r.no}</td>
      <td><strong>${r.pid}</strong></td>
      <td><span class="act-badge ${r.activity}">${r.activity || '—'}</span></td>
      <td>${r.bpm > 0 ? r.bpm + ' bpm' : '—'}</td>
      <td>${r.cDuduk}</td><td>${r.cBerjalan}</td><td>${r.cBerlari}</td>
      <td>${r.totalSamp}</td><td>${r.waktu}</td>
    </tr>
  `).join('');
}

function updateHistoryStats() {
  const total = historyRecords.length;
  const nDuduk = historyRecords.filter(r => r.activity === 'DUDUK').length;
  const nJalan = historyRecords.filter(r => r.activity === 'BERJALAN').length;
  const nLari = historyRecords.filter(r => r.activity === 'BERLARI').length;

  document.getElementById('hstatTotal').textContent = `${total} Peserta`;
  document.getElementById('hstatDuduk').textContent = `Duduk: ${nDuduk}`;
  document.getElementById('hstatJalan').textContent = `Jalan: ${nJalan}`;
  document.getElementById('hstatLari').textContent = `Lari: ${nLari}`;
}

window.clearHistory = function() {
  if (confirm('Hapus semua riwayat peserta?')) {
    historyRecords = [];
    saveHistoryToStorage(historyRecords);
    renderHistory();
    updateHistoryStats();
  }
};

// ============================================================
// CHARTS - DIPERBAIKI AGAR LEBIH TAJAM
// ============================================================
function initSensorChart() {
  const ctx = document.getElementById('sensorChart').getContext('2d');
  
  // Set canvas resolution lebih tinggi
  const canvas = document.getElementById('sensorChart');
  const container = canvas.parentElement;
  const width = container.clientWidth;
  const height = 300;
  
  canvas.width = width * 2;
  canvas.height = height * 2;
  canvas.style.width = width + 'px';
  canvas.style.height = height + 'px';
  
  sensorChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: sensorBuffer.labels,
      datasets: [
        {
          label: 'Accel (g)',
          data: sensorBuffer.accel,
          borderColor: '#60A5FA',
          backgroundColor: 'transparent',
          borderWidth: 2,
          pointRadius: 0,
          pointHoverRadius: 4,
          tension: 0.2,
          fill: false,
        },
        {
          label: 'Gyro (°/s ÷10)',
          data: sensorBuffer.gyro,
          borderColor: '#34D399',
          backgroundColor: 'transparent',
          borderWidth: 2,
          pointRadius: 0,
          pointHoverRadius: 4,
          tension: 0.2,
          fill: false,
        },
        {
          label: 'BPM ÷100',
          data: sensorBuffer.bpm,
          borderColor: '#F87171',
          backgroundColor: 'transparent',
          borderWidth: 2,
          pointRadius: 0,
          pointHoverRadius: 4,
          tension: 0.2,
          fill: false,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      animation: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: 'rgba(0,0,0,0.8)',
          titleColor: '#fff',
          bodyColor: '#ccc',
          padding: 8,
          cornerRadius: 6,
        },
      },
      scales: {
        x: {
          grid: { color: 'rgba(128,128,128,0.1)', drawBorder: true },
          ticks: { color: '#9CA3AF', font: { size: 10, family: 'JetBrains Mono' }, maxTicksLimit: 8 },
        },
        y: {
          grid: { color: 'rgba(128,128,128,0.1)' },
          ticks: { color: '#9CA3AF', font: { size: 10, family: 'JetBrains Mono' }, callback: v => v.toFixed(2) },
          suggestedMin: 0,
          suggestedMax: 1.5,
        },
      },
    },
  });
}

function initActivityChart() {
  const ctx = document.getElementById('activityChart').getContext('2d');
  
  const canvas = document.getElementById('activityChart');
  const container = canvas.parentElement;
  const width = container.clientWidth;
  const height = 220;
  
  canvas.width = width * 2;
  canvas.height = height * 2;
  canvas.style.width = width + 'px';
  canvas.style.height = height + 'px';
  
  activityChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: ['Duduk', 'Berjalan', 'Berlari'],
      datasets: [{
        data: [0, 0, 0],
        backgroundColor: ['rgba(96,165,250,0.8)', 'rgba(52,211,153,0.8)', 'rgba(248,113,113,0.8)'],
        borderColor: ['#60A5FA', '#34D399', '#F87171'],
        borderWidth: 1,
        borderRadius: 8,
        barPercentage: 0.6,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      animation: { duration: 300 },
      plugins: {
        legend: { display: false },
        tooltip: { backgroundColor: 'rgba(0,0,0,0.8)', titleColor: '#fff', bodyColor: '#ccc', padding: 8, cornerRadius: 6 },
      },
      scales: {
        x: { grid: { display: false }, ticks: { color: '#9CA3AF', font: { size: 11, weight: '500' } } },
        y: { grid: { color: 'rgba(128,128,128,0.1)' }, ticks: { color: '#9CA3AF', font: { size: 10 }, stepSize: 1, precision: 0 }, beginAtZero: true },
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
    sensorBuffer.labels.shift();
    sensorBuffer.accel.shift();
    sensorBuffer.gyro.shift();
    sensorBuffer.bpm.shift();
  }
  
  if (sensorChart) {
    sensorChart.update('none');
  }
}

function pushActivityResult(activity) {
  const now = Date.now();
  const cutoff = now - 60 * 60 * 1000;
  activityHistory.push({ time: now, activity });
  while (activityHistory.length > 0 && activityHistory[0].time < cutoff) activityHistory.shift();
  
  const c = { DUDUK: 0, BERJALAN: 0, BERLARI: 0 };
  activityHistory.forEach(r => { if (c[r.activity] !== undefined) c[r.activity]++; });
  
  if (activityChart) {
    activityChart.data.datasets[0].data = [c.DUDUK, c.BERJALAN, c.BERLARI];
    activityChart.update();
  }
}

// ============================================================
// UI UPDATES
// ============================================================
const ACTIVITY_META = {
  DUDUK: { label: 'DUDUK', icon: '🪑', sub: 'Sedang duduk / diam', color: '#60A5FA' },
  BERJALAN: { label: 'BERJALAN', icon: '🚶', sub: 'Sedang berjalan kaki', color: '#34D399' },
  BERLARI: { label: 'BERLARI', icon: '🏃', sub: 'Sedang berlari', color: '#F87171' },
};

let lastActivity = '';

function updateActivity(activity) {
  if (activity === lastActivity) return;
  lastActivity = activity;
  const meta = ACTIVITY_META[activity] || { label: activity, icon: '❓', sub: '', color: '#9CA3AF' };
  document.getElementById('activityValue').textContent = meta.label;
  document.getElementById('actIcon').textContent = meta.icon;
  document.getElementById('actSub').textContent = meta.sub;
}

function updateBPM(bpm) {
  const el = document.getElementById('bpmValue');
  const st = document.getElementById('bpmStatus');
  const bar = document.getElementById('bpmBar');
  
  if (bpm <= 0) {
    el.textContent = '--';
    st.textContent = 'Tidak terbaca';
    bar.style.width = '0%';
    return;
  }
  
  el.textContent = bpm;
  let label = 'Normal';
  if (bpm < 60) label = 'Terlalu Lambat';
  else if (bpm >= 140) label = 'Sangat Tinggi';
  else if (bpm >= 100) label = 'Tinggi';
  st.textContent = label;
  bar.style.width = Math.min(100, (bpm / 200) * 100) + '%';
}

function updateAccuracy(confidence) {
  const pct = Math.round(confidence * 100);
  const ring = document.getElementById('ringFill');
  const circumference = 2 * Math.PI * 32;
  ring.style.strokeDasharray = circumference;
  ring.style.strokeDashoffset = circumference - (pct / 100) * circumference;
  ring.style.stroke = pct >= 85 ? '#34D399' : pct >= 65 ? '#FBBF24' : '#F87171';
  document.getElementById('accuracyValue').textContent = pct + '%';
}

function updateInfo(data) {
  if (data.device_id) document.getElementById('infoDevice').textContent = data.device_id;
  if (data.participant_id || data.user) document.getElementById('infoUser').textContent = data.participant_id || data.user;
  document.getElementById('infoLastMsg').textContent = new Date().toLocaleTimeString('id-ID');
  document.getElementById('infoTotal').textContent = msgCount;
}

function setConnectionStatus(online) {
  const dot = document.getElementById('badgeDot');
  const badge = document.getElementById('badgeLabel');
  const sd = document.getElementById('statusDot');
  const st = document.getElementById('statusText');
  
  if (online) {
    dot.className = 'badge-dot online';
    badge.textContent = 'Connected';
    sd.className = 'status-dot online';
    st.textContent = 'Connected';
  } else {
    dot.className = 'badge-dot';
    badge.textContent = 'Disconnected';
    sd.className = 'status-dot';
    st.textContent = 'Disconnected';
  }
}

// ============================================================
// MQTT CONNECTION
// ============================================================
function connectMQTT() {
  const wsUrl = `ws://${CONFIG.broker}:${CONFIG.port}${CONFIG.path}`;
  console.log('[MQTT] Connecting to:', wsUrl);
  
  const client = mqtt.connect(wsUrl, {
    clientId: CONFIG.clientId,
    connectTimeout: 10000,
    reconnectPeriod: 5000,
  });

  client.on('connect', () => {
    console.log('[MQTT] Connected to EMQX Cloud!');
    setConnectionStatus(true);
    Object.values(CONFIG.topics).forEach(t => client.subscribe(t));
  });

  client.on('close', () => setConnectionStatus(false));
  client.on('error', (err) => console.error('[MQTT] Error:', err.message));

  client.on('message', (topic, payload) => {
    let data;
    try { data = JSON.parse(payload.toString()); } catch(e) { return; }

    if (topic === CONFIG.topics.sensorData) {
      pushSensorData(data.accel_stddev || 0, data.gyro_stddev || 0, data.bpm || 0);
      updateBPM(data.bpm || 0);
      updateInfo(data);
      if (data.local_act) updateActivity(data.local_act.toUpperCase());
    }
    else if (topic === CONFIG.topics.classification) {
      const act = (data.activity || '').toUpperCase();
      updateActivity(act);
      pushActivityResult(act);
      updateAccuracy(data.confidence || 0);
      if (data.bpm > 0) updateBPM(data.bpm);
    }
    else if (topic === CONFIG.topics.status) {
      if (data.final_activity) updateActivity(data.final_activity.toUpperCase());
      addHistoryRecord(data);
    }
  });
}

// ============================================================
// INITIALIZATION
// ============================================================
document.addEventListener('DOMContentLoaded', () => {
  initSensorChart();
  initActivityChart();
  
  document.getElementById('infobroker').textContent = `${CONFIG.broker}:${CONFIG.port}`;
  
  renderHistory();
  updateHistoryStats();
  
  connectMQTT();
});