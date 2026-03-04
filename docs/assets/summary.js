// ==========================================================================
//  UUID Benchmark Dashboard — Summary View
//  KPI cards, database cards, UUID legend, methodology toggle.
// ==========================================================================

import {
  KEY_TYPE_COLORS, KEY_TYPE_ORDER, KEY_TYPE_LABELS, KEY_TYPE_SHORT,
  DATABASE_COLORS,
} from './constants.js';
import { allEntries } from './data.js';
import { buildMiniBarChart } from './charts.js';

let miniCharts = [];

export function initSummary() {
  renderLegend();
  renderKPICharts();
  bindExploreButtons();
  bindKPICards();
  bindMethodologyToggle();
}

export function destroySummary() {
  miniCharts.forEach(c => { if (c) c.destroy(); });
  miniCharts = [];
}

// --- UUID types legend ---
function renderLegend() {
  const container = document.getElementById('summary-legend');
  if (!container) return;
  container.innerHTML = '';

  KEY_TYPE_ORDER.forEach(kt => {
    const item = document.createElement('span');
    item.className = 'legend-item';

    const swatch = document.createElement('span');
    swatch.className = 'legend-swatch';
    swatch.style.background = KEY_TYPE_COLORS[kt] || '#999';

    const label = document.createTextNode(KEY_TYPE_LABELS[kt] || kt);

    item.appendChild(swatch);
    item.appendChild(label);
    container.appendChild(item);
  });
}

// --- KPI sparkline charts ---
function renderKPICharts() {
  const canvases = document.querySelectorAll('.kpi-chart');
  if (canvases.length < 4) return;

  // Card 1: INSERT PENALTY — UUIDv4 throughput at 100K, 1M, 10M (PostgreSQL)
  const insertVals = getScaleValues('postgres', 'insert_performance', 1, 'throughput', 'UUIDV4');
  miniCharts.push(createMini(canvases[0], insertVals, '#1d4ed8'));

  // Card 2: STRUCTURAL DAMAGE — UUIDv4 fragmentation at 100K, 1M, 10M (PostgreSQL)
  const fragVals = getScaleValues('postgres', 'insert_performance', 1, 'fragmentation', 'UUIDV4');
  miniCharts.push(createMini(canvases[1], fragVals, '#1d4ed8'));

  // Card 3: SCALE EFFECT — UUIDv4 throughput penalty % at 100K, 1M, 10M (MySQL)
  const scaleVals = getScalePenalty('mysql', 'insert_performance', 1, 'throughput', 'UUIDV4');
  miniCharts.push(createMini(canvases[2], scaleVals, '#1d4ed8'));

  // Card 4: BEST BALANCE — UUIDv7 throughput as % of baseline across 4 databases at 1M
  const engineVals = getDatabaseComparison('UUIDV7', 'insert_performance', '1m', 1, 'throughput');
  const dbColors = [
    DATABASE_COLORS.postgres,
    DATABASE_COLORS.mysql,
    DATABASE_COLORS.mongodb,
    DATABASE_COLORS.cassandra,
  ];
  miniCharts.push(createMiniMultiColor(canvases[3], engineVals, dbColors));
}

function createMini(canvas, values, color) {
  if (!values || values.length === 0) return null;
  const config = buildMiniBarChart(values, [color + '99', color + 'bb', color]);
  return new Chart(canvas, config);
}

function createMiniMultiColor(canvas, values, colors) {
  if (!values || values.length === 0) return null;
  const config = buildMiniBarChart(values, colors.slice(0, values.length));
  return new Chart(canvas, config);
}

// --- Data query helpers ---
function getScaleValues(database, scenario, connections, metric, keyType) {
  const scales = ['100k', '1m', '10m'];
  return scales.map(scale => {
    const entry = allEntries.find(e =>
      e.database === database && e.scenario === scenario &&
      e.connections === connections && e.metric === metric &&
      e.keyType === keyType && e.scale === scale
    );
    return entry ? entry.median : null;
  });
}

function getScalePenalty(database, scenario, connections, metric, keyType) {
  const scales = ['100k', '1m', '10m'];
  return scales.map(scale => {
    const entry = allEntries.find(e =>
      e.database === database && e.scenario === scenario &&
      e.connections === connections && e.metric === metric &&
      e.keyType === keyType && e.scale === scale
    );
    const baseline = allEntries.find(e =>
      e.database === database && e.scenario === scenario &&
      e.connections === connections && e.metric === metric &&
      e.keyType === 'SEQUENTIAL' && e.scale === scale
    );
    if (entry && baseline && baseline.median !== 0) {
      return Math.abs(((entry.median - baseline.median) / baseline.median) * 100);
    }
    return null;
  });
}

function getDatabaseComparison(keyType, scenario, scale, connections, metric) {
  const dbs = ['postgres', 'mysql', 'mongodb', 'cassandra'];
  return dbs.map(db => {
    const entry = allEntries.find(e =>
      e.database === db && e.scenario === scenario &&
      e.connections === connections && e.metric === metric &&
      e.keyType === keyType && e.scale === scale
    );
    const baseline = allEntries.find(e =>
      e.database === db && e.scenario === scenario &&
      e.connections === connections && e.metric === metric &&
      e.keyType === 'SEQUENTIAL' && e.scale === scale
    );
    if (entry && baseline && baseline.median !== 0) {
      return (entry.median / baseline.median) * 100;
    }
    return null;
  });
}

// --- Explore buttons ---
function bindExploreButtons() {
  document.querySelectorAll('.db-explore').forEach(btn => {
    btn.addEventListener('click', () => {
      const db = btn.dataset.db;
      if (window.navigateToExplorer) {
        window.navigateToExplorer({ database: db });
      }
    });
  });
}

// --- KPI card clicks ---
function bindKPICards() {
  document.querySelectorAll('.kpi-card').forEach(card => {
    const handler = () => {
      const finding = card.dataset.finding;
      const filters = {};

      switch (finding) {
        case 'insert':
          filters.database = 'postgres';
          filters.scenario = 'insert_performance';
          filters.scale = '1m';
          filters.connections = 1;
          break;
        case 'structural':
          filters.database = 'postgres';
          filters.scenario = 'insert_performance';
          filters.scale = '1m';
          filters.connections = 1;
          break;
        case 'scale':
          filters.database = 'mysql';
          filters.scenario = 'insert_performance';
          filters.connections = 1;
          break;
        case 'engine':
          filters.scenario = 'insert_performance';
          filters.scale = '1m';
          filters.connections = 1;
          break;
      }

      if (window.navigateToExplorer) {
        window.navigateToExplorer(filters, finding === 'scale' ? 'scale' : undefined);
      }
    };

    card.addEventListener('click', handler);
    card.addEventListener('keydown', e => {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        handler();
      }
    });
  });
}

// --- Methodology toggle ---
function bindMethodologyToggle() {
  const toggle = document.querySelector('.methodology-toggle');
  const detail = document.getElementById('methodology-detail');
  if (!toggle || !detail) return;

  toggle.addEventListener('click', () => {
    const expanded = detail.hidden;
    detail.hidden = !expanded;
    toggle.setAttribute('aria-expanded', String(expanded));
    toggle.innerHTML = expanded ? '&#9666; Hide' : '&#9656; Details';
  });
}
