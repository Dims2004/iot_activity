/**
 * script.js — AIoT Watch Dashboard
 *
 * MQTT WebSocket: broker.emqx.io:8083/mqtt (Cloud Broker)
 * Bisa diakses dari mana saja tanpa perlu Mosquitto lokal
 */

'use strict';

// ═══════════════════════════════════════════════════════════
//  CONFIG — CLOUD BROKER EMQX
// ═══════════════════════════════════════════════════════════
const CONFIG = {
  broker:   'broker.emqx.io',      // Cloud broker EMQX (gratis)
  port:     8083,                   // WebSocket port untuk EMQX
  path:     '/mqtt',                // WebSocket path
  clientId: 'dashboard_' + Math.random().toString(16).slice(2, 10),
  topics: {
    sensorData:     'sensor/esp32/data',
    classification: 'classification/result',
    status:         'status/esp32',
  },
};

const STORAGE_KEY = 'aiot_history_cloud_v2';   // localStorage key untuk history

// ═══════════════════════════════════════════════════════════
//  SENSOR CHART
// ═══════════════════════════════════════════════════════════
const MAX_POINTS = 400;
const sensorBuffer = { labels: [], accel: [], gyro: [], bpm: [] };
let sensorChart   = null;
let activityChart = null;
let msgCount      = 0;

const activityHistory = [];   // distribusi aktivitas 1 jam terakhir

// ═══════════════════════════════════════════════════════════
//  HISTORY — PERSISTEN VIA localStorage
// ═══════════════════════════════════════════════════════════

/** Muat riwayat dari localStorage saat halaman dibuka */
function loadHistoryFromStorage() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch (e) {
    console.warn('[History] Gagal muat dari localStorage:', e);
    return [];
  }
}

/** Simpan seluruh array history ke localStorage */
function saveHistoryToStorage(records) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(records));
  } catch (e) {
    console.warn('[History] Gagal simpan ke localStorage:', e);
  }
}

// Array di-memory (diisi dari localStorage saat boot)
let historyRecords = loadHistoryFromStorage();

/** Tambah satu record baru ke history, simpan ke storage, render ulang */
function addHistoryRecord(data) {
  const no        = parseInt(data.participant_no)  || (historyRecords.length + 1);
  const pid       = data.participant_id || data.user || '—';
  const activity  = (data.final_activity || '').toUpperCase();
  const bpm       = parseInt(data.final_bpm)       || 0;
  const cDuduk    = parseInt(data.count_duduk)      || 0;
  const cBerjalan = parseInt(data.count_berjalan)   || 0;
  const cBerlari  = parseInt(data.count_berlari)    || 0;
  const totalSamp = parseInt(data.total_samples)    || (cDuduk + cBerjalan + cBerlari);
  const waktu     = new Date().toLocaleTimeString('id-ID', { hour:'2-digit', minute:'2-digit', second:'2-digit' });

  // Cek duplikat berdasarkan participant_no + participant_id
  const isDuplicate = historyRecords.some(
    r => r.no === no && r.pid === pid
  );
  if (isDuplicate) {
    console.log(`[History] P${no} ${pid} sudah ada, skip.`);
    return;
  }

  const record = { no, pid, activity, bpm, cDuduk, cBerjalan, cBerlari, totalSamp, waktu };
  // Tambahkan di awal array (terbaru di atas)
  historyRecords.unshift(record);

  // Simpan ke localStorage
  saveHistoryToStorage(historyRecords);

  renderHistory(true);       // true = animasi baris baru
  updateHistoryStats();
}

/** Render tabel dari historyRecords (terbaru di atas) */
function renderHistory(animateNew = false) {
  const tbody = document.getElementById('historyBody');
  if (!tbody) return;

  if (historyRecords.length === 0) {
    tbody.innerHTML = '<tr class="history-empty"><td colspan="9">Menunggu hasil sesi peserta pertama...</td></tr>';
    return;
  }

  // Tidak perlu sorting lagi karena sudah unshift
  tbody.innerHTML = historyRecords.map((r, i) => `
    <tr class="${animateNew && i === 0 ? 'new-row' : ''}">
      <td>${r.no}</td>
      <td><strong>${r.pid}</strong></td>
      <td><span class="act-badge ${r.activity}">${r.activity || '—'}</span></td>
      <td>${r.bpm > 0 ? r.bpm + ' bpm' : '—'}</td>
      <td>${r.cDuduk}</td>
      <td>${r.cBerjalan}</td>
      <td>${r.cBerlari}</td>
      <td>${r.totalSamp}</td>
      <td>${r.waktu}</td>
    </tr>
  `).join('');
}

/** Update counter badge di atas tabel */
function updateHistoryStats() {
  const total   = historyRecords.length;
  const nDuduk  = historyRecords.filter(r => r.activity === 'DUDUK').length;
  const nJalan  = historyRecords.filter(r => r.activity === 'BERJALAN').length;
  const nLari   = historyRecords.filter(r => r.activity === 'BERLARI').length;

  const set = (id, txt) => { const e = document.getElementById(id); if (e) e.textContent = txt; };
  set('hstatTotal', `${total} Peserta`);
  set('hstatDuduk', `Duduk: ${nDuduk}`);
  set('hstatJalan', `Jalan: ${nJalan}`);
  set('hstatLari',  `Lari: ${nLari}`);
}

/** Hapus semua history (confirm dulu) — dipanggil dari tombol HTML */
function clearHistory() {
  if (!confirm('Hapus semua riwayat peserta dari tampilan dan penyimpanan lokal?')) return;
  historyRecords = [];
  saveHistoryToStorage(historyRecords);
  renderHistory();
  updateHistoryStats();
}

// Expose ke global agar onclick di HTML bisa akses
window.clearHistory = clearHistory;

// ═══════════════════════════════════════════════════════════
//  CHART — Sensor line chart
// ═══════════════════════════════════════════════════════════
function initSensorChart() {
  const ctx = document.getElementById('sensorChart').getContext('2d');
  sensorChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels:   sensorBuffer.labels,
      datasets: [
        {
          label: 'Accel', data: sensorBuffer.accel,
          borderColor: '#60A5FA', backgroundColor: 'rgba(96,165,250,0.07)',
          borderWidth: 1.8, pointRadius: 0, tension: 0.4, fill: true,
        },
        {
          label: 'Gyro÷10', data: sensorBuffer.gyro,
          borderColor: '#34D399', backgroundColor: 'rgba(52,211,153,0.05)',
          borderWidth: 1.8, pointRadius: 0, tension: 0.4, fill: true,
        },
        {
          label: 'BPM÷100', data: sensorBuffer.bpm,
          borderColor: '#F87171', backgroundColor: 'rgba(248,113,113,0.05)',
          borderWidth: 1.8, pointRadius: 0, tension: 0.4, fill: true,
        },
      ],
    },
    options: {
      responsive: true, maintainAspectRatio: false, animation: { duration: 0 },
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: 'rgba(10,15,46,0.88)',
          titleColor: '#A5B4FC', bodyColor: '#E5E7EB',
          padding: 10, cornerRadius: 8,
          callbacks: {
            title: (i) => `Sample #${i[0].label}`,
            label: (i) => {
              if (i.datasetIndex === 0) return `  Accel: ${(i.raw).toFixed(4)} g`;
              if (i.datasetIndex === 1) return `  Gyro : ${(i.raw * 10).toFixed(2)} °/s`;
              return `  BPM  : ${Math.round(i.raw * 100)}`;
            },
          },
        },
      },
      scales: {
        x: { grid: { color: 'rgba(0,0,0,0.05)' }, ticks: { color: '#9CA3AF', font: { size: 10 }, maxTicksLimit: 10 } },
        y: { grid: { color: 'rgba(0,0,0,0.05)' }, ticks: { color: '#9CA3AF', font: { size: 10 }, callback: v => v.toFixed(2) }, suggestedMin: 0, suggestedMax: 1.5 },
      },
    },
  });
}

// ═══════════════════════════════════════════════════════════
//  CHART — Activity bar chart
// ═══════════════════════════════════════════════════════════
function initActivityChart() {
  const ctx = document.getElementById('activityChart').getContext('2d');
  activityChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels:   ['Duduk', 'Berjalan', 'Berlari'],
      datasets: [{
        data: [0, 0, 0],
        backgroundColor: ['rgba(96,165,250,0.75)', 'rgba(52,211,153,0.75)', 'rgba(248,113,113,0.75)'],
        borderColor:     ['#60A5FA', '#34D399', '#F87171'],
        borderWidth: 1.5, borderRadius: 6,
      }],
    },
    options: {
      responsive: true, maintainAspectRatio: false, animation: { duration: 400 },
      plugins: {
        legend: { display: false },
        tooltip: { backgroundColor: 'rgba(10,15,46,0.88)', titleColor: '#A5B4FC', bodyColor: '#E5E7EB', padding: 10, cornerRadius: 8 },
      },
      scales: {
        x: { grid: { display: false }, ticks: { color: '#6B7280', font: { size: 11, weight: '600' } } },
        y: { grid: { color: 'rgba(0,0,0,0.05)' }, ticks: { color: '#9CA3AF', font: { size: 10 }, stepSize: 1, precision: 0 }, beginAtZero: true },
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
  const now    = Date.now();
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

// ═══════════════════════════════════════════════════════════
//  UI — Activity, BPM, Accuracy, Info
// ═══════════════════════════════════════════════════════════
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
  const el  = document.getElementById('activityValue');
  const ic  = document.getElementById('actIcon');
  const sub = document.getElementById('actSub');
  if (el) { el.textContent = meta.label; el.style.color = meta.color; }
  if (ic) { ic.textContent = meta.icon; ic.style.transform = 'scale(1.4)'; setTimeout(() => ic.style.transform = 'scale(1)', 200); }
  if (sub) sub.textContent = meta.sub;
}

function updateBPM(bpm) {
  const el  = document.getElementById('bpmValue');
  const st  = document.getElementById('bpmStatus');
  const bar = document.getElementById('bpmBar');
  const glow= document.getElementById('bpmGlow');
  if (!el) return;
  if (bpm <= 0) {
    el.textContent = '--'; if (st) { st.textContent = 'Tidak terbaca'; st.className = 'bpm-status'; }
    if (bar) bar.style.width = '0%'; return;
  }
  el.textContent = bpm;
  let label = 'Normal', cls = 'bpm-status';
  if      (bpm < 60)  { label = 'Terlalu Lambat'; cls = 'bpm-status warn'; }
  else if (bpm >= 140) { label = 'Sangat Tinggi'; cls = 'bpm-status danger'; }
  else if (bpm >= 100) { label = 'Tinggi';        cls = 'bpm-status warn'; }
  if (st) { st.textContent = label; st.className = cls; }
  if (bar) bar.style.width = Math.min(100, (bpm / 200) * 100) + '%';
  if (glow) glow.style.background = `rgba(255,107,107,${0.15 + (bpm/200)*0.35})`;
}

function updateAccuracy(confidence) {
  const pct  = Math.round(confidence * 100);
  const ring = document.getElementById('ringFill');
  const text = document.getElementById('accuracyValue');
  const circumference = 2 * Math.PI * 32;
  if (ring) {
    ring.style.strokeDasharray = circumference;
    ring.style.strokeDashoffset = circumference - (pct / 100) * circumference;
    ring.style.stroke = pct >= 85 ? '#34D399' : pct >= 65 ? '#F5A623' : '#F87171';
  }
  if (text) text.textContent = pct + '%';
}

function updateInfo(data) {
  const set = (id, v) => { const e = document.getElementById(id); if (e && v) e.textContent = v; };
  set('infoDevice',  data.device_id);
  set('infoUser',    data.participant_id || data.user);
  set('infoLastMsg', new Date().toLocaleTimeString('id-ID'));
  const tot = document.getElementById('infoTotal');
  if (tot) tot.textContent = msgCount;
}

function setConnectionStatus(online, label = '') {
  const dot   = document.getElementById('badgeDot');
  const badge = document.getElementById('badgeLabel');
  const sd    = document.getElementById('statusDot');
  const st    = document.getElementById('statusText');
  if (online) {
    if (dot)   dot.className   = 'badge-dot online';
    if (badge) badge.textContent = 'Connected';
    if (sd)    sd.className    = 'status-dot online';
    if (st)  { st.textContent  = 'Connected'; st.style.color = '#22C55E'; }
  } else {
    if (dot)   dot.className   = 'badge-dot offline';
    if (badge) badge.textContent = label || 'Disconnected';
    if (sd)    sd.className    = 'status-dot offline';
    if (st)  { st.textContent  = label || 'Disconnected'; st.style.color = '#EF4444'; }
  }
}

// ═══════════════════════════════════════════════════════════
//  DEMO MODE (fallback jika MQTT gagal)
// ═══════════════════════════════════════════════════════════
let demoInterval = null;

function startDemoMode() {
  console.warn('[DEMO] MQTT tidak terhubung — data simulasi.');
  setConnectionStatus(false, 'Demo Mode');
  const acts = ['DUDUK', 'BERJALAN', 'BERLARI'];
  let t = 0;
  demoInterval = setInterval(() => {
    t += 0.1;
    const act = acts[Math.floor(t / 20) % 3];
    let accel, gyro, bpm;
    if      (act === 'DUDUK')    { accel = 0.01 + Math.random()*0.02; gyro = 3  + Math.random()*8;   bpm = 68  + Math.random()*8; }
    else if (act === 'BERJALAN') { accel = 0.07 + Math.random()*0.08; gyro = 20 + Math.random()*30;  bpm = 88  + Math.random()*12; }
    else                         { accel = 0.25 + Math.random()*0.15; gyro = 75 + Math.random()*50;  bpm = 130 + Math.random()*20; }
    pushSensorData(accel, gyro, Math.round(bpm));
    pushActivityResult(act);
    updateActivity(act);
    updateBPM(Math.round(bpm));
    updateAccuracy(0.88 + Math.random()*0.10);
    updateInfo({ device_id: 'ESP32_001', participant_id: 'Demo' });
  }, 200);
}

function stopDemoMode() { if (demoInterval) { clearInterval(demoInterval); demoInterval = null; } }

// ═══════════════════════════════════════════════════════════
//  MQTT — CLOUD BROKER EMQX
// ═══════════════════════════════════════════════════════════
function connectMQTT() {
  let client;
  const wsUrl = `ws://${CONFIG.broker}:${CONFIG.port}${CONFIG.path}`;
  console.log(`[MQTT] Menghubungkan ke cloud broker: ${wsUrl}`);
  
  try {
    client = mqtt.connect(wsUrl, {
      clientId: CONFIG.clientId,
      connectTimeout: 10000,
      reconnectPeriod: 5000,
    });
  } catch (e) { 
    console.error('[MQTT] Error koneksi:', e);
    startDemoMode(); 
    return; 
  }

  const timeout = setTimeout(() => {
    if (!client.connected) { 
      console.warn('[MQTT] Timeout koneksi');
      client.end(true); 
      startDemoMode(); 
    }
  }, 10000);

  client.on('connect', () => {
    clearTimeout(timeout);
    stopDemoMode();
    setConnectionStatus(true);
    console.log('[MQTT] Terhubung ke EMQX Cloud!');
    Object.values(CONFIG.topics).forEach(t => {
      client.subscribe(t, { qos: 0 });
      console.log(`[MQTT] Subscribe ke: ${t}`);
    });
  });

  client.on('close', () => {
    console.log('[MQTT] Koneksi ditutup');
    setConnectionStatus(false, 'Reconnecting…');
  });
  
  client.on('error', (err) => {
    console.error('[MQTT] Error:', err.message);
    setConnectionStatus(false, 'Error');
  });

  client.on('message', (topic, payload) => {
    let data;
    try { data = JSON.parse(payload.toString()); } catch(e) { return; }

    if (topic === CONFIG.topics.sensorData) {
      const accel = parseFloat(data.accel_stddev) || 0;
      const gyro  = parseFloat(data.gyro_stddev)  || 0;
      const bpm   = parseInt(data.bpm, 10)         || 0;
      pushSensorData(accel, gyro, bpm);
      updateBPM(bpm);
      updateInfo(data);
      if (data.local_act) updateActivity(data.local_act.toUpperCase());
    }

    else if (topic === CONFIG.topics.classification) {
      const act  = (data.activity || '').toUpperCase();
      const conf = parseFloat(data.confidence) || 0;
      const bpm  = parseInt(data.bpm, 10) || 0;
      updateActivity(act);
      pushActivityResult(act);
      updateAccuracy(conf);
      if (bpm > 0) updateBPM(bpm);
    }

    else if (topic === CONFIG.topics.status) {
      console.log('[STATUS AKHIR]', data);
      if (data.final_activity) {
        updateActivity(data.final_activity.toUpperCase());
      }
      addHistoryRecord(data);
    }
  });
}

// ═══════════════════════════════════════════════════════════
//  BOOT
// ═══════════════════════════════════════════════════════════
document.addEventListener('DOMContentLoaded', () => {
  // Inisialisasi chart
  initSensorChart();
  initActivityChart();

  // Broker info
  const bi = document.getElementById('infobroker');
  if (bi) bi.textContent = `${CONFIG.broker}:${CONFIG.port}`;

  // Render history dari localStorage (agar tidak hilang setelah refresh)
  renderHistory();
  updateHistoryStats();

  // Connect MQTT ke cloud broker
  connectMQTT();
});