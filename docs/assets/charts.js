// ==========================================================================
//  UUID Benchmark Dashboard — Chart.js Builders
//  Monochrome terminal aesthetic: JetBrains Mono, #111 text, #ccc grid.
// ==========================================================================

import {
  KEY_TYPE_COLORS, KEY_TYPE_DASH, KEY_TYPE_POINT_STYLE, KEY_TYPE_ORDER,
  DATABASE_COLORS, DATABASE_ORDER, SCALE_ORDER, KEY_TYPE_LABELS,
  formatKeyTypeName, formatDatabaseName, formatNumber, METRIC_LABELS,
} from './constants.js';

const CHART_FONT = '"JetBrains Mono", "Courier New", Courier, monospace';
const CHART_ANIMATION = { duration: 300 };
const CHART_GRID_STYLE = { color: 'rgba(0, 0, 0, 0.06)' };
const CHART_BORDER_STYLE = { color: '#333333', display: true };

const TOOLTIP_CONFIG = {
  backgroundColor: '#ffffff',
  borderColor: '#cccccc',
  borderWidth: 1,
  titleColor: '#111111',
  bodyColor: '#444444',
  cornerRadius: 0,
  titleFont: { family: CHART_FONT, size: 12, weight: '700' },
  bodyFont: { family: CHART_FONT, size: 11 },
  padding: 8,
};

// --- Error bar plugin ---
export const errorBarPlugin = {
  id: 'errorBars',
  afterDraw(chart) {
    const ctx = chart.ctx;
    chart.data.datasets.forEach((dataset, dsIndex) => {
      if (!dataset.errorBars) return;
      const meta = chart.getDatasetMeta(dsIndex);
      if (meta.hidden) return;

      dataset.errorBars.forEach((eb, index) => {
        if (!eb || eb.low == null || eb.high == null) return;
        const element = meta.data[index];
        if (!element) return;

        const x = element.x;
        const yScale = chart.scales.y;
        const yLow = yScale.getPixelForValue(eb.low);
        const yHigh = yScale.getPixelForValue(eb.high);
        const capWidth = 5;

        ctx.save();
        ctx.strokeStyle = '#333333';
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(x, yLow);
        ctx.lineTo(x, yHigh);
        ctx.moveTo(x - capWidth, yHigh);
        ctx.lineTo(x + capWidth, yHigh);
        ctx.moveTo(x - capWidth, yLow);
        ctx.lineTo(x + capWidth, yLow);
        ctx.stroke();
        ctx.restore();
      });
    });
  },
};

// --- Shared scale config ---
function xScale() {
  return {
    grid: { display: false },
    border: CHART_BORDER_STYLE,
    ticks: {
      font: { size: 11, family: CHART_FONT },
      color: '#888888',
      maxRotation: 45,
      autoSkip: false,
    },
  };
}

function yScale(beginAtZero = true) {
  return {
    beginAtZero,
    grid: CHART_GRID_STYLE,
    border: CHART_BORDER_STYLE,
    ticks: {
      maxTicksLimit: 6,
      font: { size: 11, family: CHART_FONT },
      color: '#888888',
    },
  };
}

// --- Cross-UUID bar chart ---
export function buildCrossUUIDChart(entries, metric) {
  const byKeyType = {};
  entries.forEach(e => { byKeyType[e.keyType] = e; });

  const labels = [];
  const data = [];
  const colors = [];
  const errorBars = [];

  KEY_TYPE_ORDER.forEach(kt => {
    if (!byKeyType[kt]) return;
    const e = byKeyType[kt];
    labels.push(formatKeyTypeName(kt));
    data.push(e.median);
    colors.push(KEY_TYPE_COLORS[kt] || '#999');
    errorBars.push({
      low: Math.max(0, e.median - e.stddev),
      high: e.median + e.stddev,
    });
  });

  if (labels.length === 0) return null;

  return {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        data,
        backgroundColor: colors.map(c => c + 'cc'),
        borderColor: colors,
        borderWidth: 1,
        errorBars,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: CHART_ANIMATION,
      plugins: {
        legend: { display: false },
        tooltip: {
          ...TOOLTIP_CONFIG,
          callbacks: {
            title: ctx => ctx[0]?.label || '',
            label(ctx) {
              const e = entries.find(en => formatKeyTypeName(en.keyType) === ctx.label);
              const lines = [formatNumber(ctx.parsed.y)];
              if (e) {
                lines.push('Mean: ' + formatNumber(e.mean));
                lines.push('\u00b1' + formatNumber(e.stddev));
              }
              return lines;
            },
          },
        },
      },
      scales: { x: xScale(), y: yScale() },
    },
  };
}

// --- Cross-DB grouped bar chart (% vs Sequential baseline) ---
export function buildCrossDBChart(allEntries, currentFilterState, metric) {
  const { scenario, scale, connections } = currentFilterState;
  if (!scenario || !scale || connections == null || !metric) return null;

  const matching = allEntries.filter(e =>
    e.scenario === scenario
    && e.scale === scale
    && String(e.connections) === String(connections)
    && e.metric === metric
  );

  const byDB = {};
  matching.forEach(e => {
    if (!byDB[e.database]) byDB[e.database] = {};
    byDB[e.database][e.keyType] = e;
  });

  const dbsWithBaseline = [];
  DATABASE_ORDER.forEach(db => {
    if (byDB[db] && byDB[db].SEQUENTIAL) dbsWithBaseline.push(db);
  });
  if (dbsWithBaseline.length === 0) return null;

  const keyTypesPresent = [];
  KEY_TYPE_ORDER.forEach(kt => {
    if (kt === 'SEQUENTIAL') return;
    if (dbsWithBaseline.some(db => byDB[db][kt] != null)) keyTypesPresent.push(kt);
  });
  if (keyTypesPresent.length === 0) return null;

  const labels = dbsWithBaseline.map(db => formatDatabaseName(db));

  const datasets = keyTypesPresent.map(kt => {
    const data = dbsWithBaseline.map(db => {
      const baseline = byDB[db].SEQUENTIAL.median;
      const entry = byDB[db][kt];
      if (entry && baseline !== 0) {
        return Math.round(((entry.median - baseline) / Math.abs(baseline)) * 10000) / 100;
      }
      return null;
    });

    const color = KEY_TYPE_COLORS[kt] || '#999';
    return {
      label: formatKeyTypeName(kt),
      data,
      backgroundColor: color + 'cc',
      borderColor: color,
      borderWidth: 1,
    };
  });

  return {
    type: 'bar',
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: CHART_ANIMATION,
      plugins: {
        legend: { display: false },
        tooltip: {
          ...TOOLTIP_CONFIG,
          callbacks: {
            label(ctx) {
              const pct = ctx.parsed.y;
              const sign = pct >= 0 ? '+' : '';
              const db = dbsWithBaseline[ctx.dataIndex];
              const kt = keyTypesPresent[ctx.datasetIndex];
              const entry = byDB[db] && byDB[db][kt];
              const baseline = byDB[db] && byDB[db].SEQUENTIAL;
              const lines = [ctx.dataset.label + ': ' + sign + formatNumber(pct) + '%'];
              if (entry) lines.push('Actual: ' + formatNumber(entry.median));
              if (baseline) lines.push('Baseline: ' + formatNumber(baseline.median));
              return lines;
            },
          },
        },
      },
      scales: {
        x: xScale(),
        y: {
          ...yScale(false),
          ticks: {
            ...yScale(false).ticks,
            callback: v => (v >= 0 ? '+' : '') + v + '%',
          },
        },
      },
    },
  };
}

// --- Scale line chart ---
export function buildScaleChart(entries, metric) {
  const scalesPresent = {};
  entries.forEach(e => { scalesPresent[e.scale] = true; });
  const scaleLabels = [];
  SCALE_ORDER.forEach(s => { if (scalesPresent[s]) scaleLabels.push(s); });
  if (scaleLabels.length === 0) return null;

  const byKeyType = {};
  entries.forEach(e => {
    if (!byKeyType[e.keyType]) byKeyType[e.keyType] = {};
    byKeyType[e.keyType][e.scale] = e;
  });

  const datasets = [];
  KEY_TYPE_ORDER.forEach(kt => {
    if (!byKeyType[kt]) return;
    const ktData = byKeyType[kt];
    const data = [];
    const errBars = [];

    scaleLabels.forEach(s => {
      if (ktData[s]) {
        data.push(ktData[s].median);
        errBars.push({
          low: Math.max(0, ktData[s].median - ktData[s].stddev),
          high: ktData[s].median + ktData[s].stddev,
        });
      } else {
        data.push(null);
        errBars.push(null);
      }
    });

    const color = KEY_TYPE_COLORS[kt] || '#999';
    datasets.push({
      label: formatKeyTypeName(kt),
      data,
      borderColor: color,
      backgroundColor: color,
      borderWidth: 2,
      borderDash: KEY_TYPE_DASH[kt] || [],
      pointRadius: 4,
      pointHoverRadius: 6,
      pointStyle: KEY_TYPE_POINT_STYLE[kt] || 'circle',
      pointBackgroundColor: color,
      pointBorderColor: '#ffffff',
      pointBorderWidth: 1.5,
      fill: false,
      spanGaps: false,
      tension: 0,
      errorBars: errBars,
    });
  });

  if (datasets.length === 0) return null;

  return {
    type: 'line',
    data: {
      labels: scaleLabels.map(s => s.toUpperCase()),
      datasets,
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: CHART_ANIMATION,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          ...TOOLTIP_CONFIG,
          callbacks: {
            label(ctx) {
              const kt = Object.keys(byKeyType).find(k => formatKeyTypeName(k) === ctx.dataset.label);
              const scaleKey = scaleLabels[ctx.dataIndex];
              const e = kt && byKeyType[kt] && byKeyType[kt][scaleKey];
              const lines = [ctx.dataset.label + ': ' + formatNumber(ctx.parsed.y)];
              if (e) lines.push('\u00b1' + formatNumber(e.stddev));
              return lines;
            },
          },
        },
      },
      scales: {
        x: {
          ...xScale(),
          title: {
            display: true,
            text: 'DATASET SCALE',
            font: { size: 11, weight: '700', family: CHART_FONT },
            color: '#888888',
          },
        },
        y: yScale(),
      },
    },
  };
}

// --- Mini bar chart for KPI cards (3 tiny bars) ---
export function buildMiniBarChart(values, colors) {
  return {
    type: 'bar',
    data: {
      labels: values.map((_, i) => i),
      datasets: [{
        data: values.map(v => v ?? 0),
        backgroundColor: colors || ['#cccccc', '#cccccc', '#cccccc'],
        borderWidth: 0,
        barPercentage: 0.8,
      }],
    },
    options: {
      responsive: false,
      maintainAspectRatio: false,
      animation: false,
      plugins: { legend: { display: false }, tooltip: { enabled: false } },
      scales: {
        x: { display: false },
        y: { display: false, beginAtZero: true },
      },
    },
  };
}
