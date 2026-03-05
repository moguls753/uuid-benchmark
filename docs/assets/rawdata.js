// ==========================================================================
//  UUID Benchmark Dashboard — Raw Data Table View
//  Sortable columns, monochrome conditional formatting.
// ==========================================================================

import {
  KEY_TYPE_ORDER, KEY_TYPE_COLORS, LOWER_IS_BETTER,
  formatKeyTypeName, formatMetricName, formatNumber,
  formatDatabaseName, formatScenarioName,
} from './constants.js';
import { allEntries, coerceFilterValue, matchesPartial } from './data.js';
import { openFilterModal, closeFilterModal } from './filterModal.js';

const TABLE_COLUMNS = [
  { key: 'keyType',  label: 'Key Type',  numeric: false },
  { key: 'database', label: 'Database',  numeric: false },
  { key: 'scenario', label: 'Scenario',  numeric: false },
  { key: 'scale',    label: 'Scale',     numeric: false },
  { key: 'metric',   label: 'Metric',    numeric: false },
  { key: 'median',   label: 'Median',    numeric: true  },
  { key: 'mean',     label: 'Mean',      numeric: true  },
  { key: 'stddev',   label: 'StdDev',    numeric: true  },
  { key: 'cv',       label: 'CV%',       numeric: true  },
  { key: 'min',      label: 'Min',       numeric: true  },
  { key: 'max',      label: 'Max',       numeric: true  },
];

const RAW_FILTERS = ['database', 'scenario', 'scale', 'connections'];

let tableSortState = { column: 'keyType', direction: 'asc' };
let rawFilterState = { database: null, scenario: null, scale: null, connections: null };
let rawDom = {};
let eventsBound = false;

export function initRawData() {
  cacheRawDom();
  populateRawFilters();
  if (!eventsBound) {
    bindRawFilters();
    bindRawFilterModalToggle();
    eventsBound = true;
  }
  renderRawData();
}

export function destroyRawData() {
  closeFilterModal();
}

function cacheRawDom() {
  rawDom.filters = {
    database: document.getElementById('raw-filter-database'),
    scenario: document.getElementById('raw-filter-scenario'),
    scale: document.getElementById('raw-filter-scale'),
    connections: document.getElementById('raw-filter-connections'),
  };
  rawDom.filterToggle = document.querySelector('#view-raw-data .raw-filter-toggle');
  rawDom.filterModalGroups = document.querySelector('#view-raw-data .filter-modal-groups');
  rawDom.thead = document.querySelector('#view-raw-data .data-table thead tr');
  rawDom.tbody = document.querySelector('#view-raw-data .data-table tbody');
  rawDom.sortInfo = document.querySelector('#view-raw-data .table-sort-info');
  rawDom.count = document.querySelector('#view-raw-data .table-count');
  rawDom.cardsContainer = document.getElementById('raw-data-cards');
}

// --- Raw filter population (with "All" option) ---
function populateRawFilters() {
  RAW_FILTERS.forEach(key => {
    const el = rawDom.filters[key];
    if (!el) return;

    const opts = deriveRawOptions(key);
    el.innerHTML = '';

    // Add "All" option
    const allOpt = document.createElement('option');
    allOpt.value = '__all__';
    allOpt.textContent = 'All';
    el.appendChild(allOpt);

    opts.forEach(val => {
      const option = document.createElement('option');
      option.value = val;
      option.textContent = formatRawOption(key, val);
      el.appendChild(option);
    });

    // Restore previous selection
    if (rawFilterState[key] != null) {
      el.value = String(rawFilterState[key]);
    } else {
      el.value = '__all__';
    }
  });
}

function deriveRawOptions(key) {
  const partial = {};
  RAW_FILTERS.forEach(k => {
    if (k !== key && rawFilterState[k] != null) {
      partial[k] = rawFilterState[k];
    }
  });

  const seen = {};
  allEntries.forEach(e => {
    if (matchesPartial(e, partial)) {
      if (e[key] != null) seen[e[key]] = true;
    }
  });

  const opts = Object.keys(seen).map(v => coerceFilterValue(key, v));
  opts.sort((a, b) => {
    if (key === 'scale') {
      const order = { '100k': 0, '1m': 1, '10m': 2, '100m': 3 };
      return (order[a] || 0) - (order[b] || 0);
    }
    if (key === 'connections') return Number(a) - Number(b);
    return String(a).localeCompare(String(b));
  });
  return opts;
}

function formatRawOption(key, val) {
  switch (key) {
    case 'database': return formatDatabaseName(val);
    case 'scenario': return formatScenarioName(val);
    case 'scale': return String(val).toUpperCase();
    case 'connections': return val === 1 ? '1 connection' : val + ' connections';
    default: return String(val);
  }
}

function bindRawFilters() {
  RAW_FILTERS.forEach(key => {
    const el = rawDom.filters[key];
    if (!el) return;
    el.addEventListener('change', () => {
      const val = el.value;
      rawFilterState[key] = val === '__all__' ? null : coerceFilterValue(key, val);
      // Cascade other filters
      RAW_FILTERS.forEach(k => {
        if (k === key) return;
        const otherEl = rawDom.filters[k];
        if (!otherEl) return;
        const opts = deriveRawOptions(k);
        const prev = rawFilterState[k];
        otherEl.innerHTML = '';
        const allOpt = document.createElement('option');
        allOpt.value = '__all__';
        allOpt.textContent = 'All';
        otherEl.appendChild(allOpt);
        opts.forEach(v => {
          const option = document.createElement('option');
          option.value = v;
          option.textContent = formatRawOption(k, v);
          otherEl.appendChild(option);
        });
        if (prev != null && opts.map(String).includes(String(prev))) {
          otherEl.value = String(prev);
        } else {
          otherEl.value = '__all__';
          rawFilterState[k] = null;
        }
      });
      renderRawData();
    });
  });
}

function bindRawFilterModalToggle() {
  if (!rawDom.filterToggle || !rawDom.filterModalGroups) return;
  rawDom.filterToggle.addEventListener('click', () => {
    openFilterModal(rawDom.filterModalGroups);
  });
}

// --- Get filtered entries for raw data ---
function getRawEntries() {
  const partial = {};
  RAW_FILTERS.forEach(k => {
    if (rawFilterState[k] != null) partial[k] = rawFilterState[k];
  });
  return allEntries.filter(e => matchesPartial(e, partial));
}

// --- Render the table ---
function renderRawData() {
  const entries = getRawEntries();

  // Build header
  rawDom.thead.innerHTML = '';
  TABLE_COLUMNS.forEach(col => {
    const th = document.createElement('th');
    th.className = col.numeric ? 'num' : '';
    th.setAttribute('data-col', col.key);

    th.appendChild(document.createTextNode(col.label + ' '));

    const indicator = document.createElement('span');
    indicator.className = 'sort-indicator';
    if (tableSortState.column === col.key) {
      th.classList.add('sort-active');
      th.setAttribute('aria-sort', tableSortState.direction === 'asc' ? 'ascending' : 'descending');
      indicator.textContent = tableSortState.direction === 'asc' ? '\u25B4' : '\u25BE';
    } else {
      th.removeAttribute('aria-sort');
      indicator.textContent = '\u25B4';
    }
    th.appendChild(indicator);

    th.addEventListener('click', () => {
      if (tableSortState.column === col.key) {
        tableSortState.direction = tableSortState.direction === 'asc' ? 'desc' : 'asc';
      } else {
        tableSortState.column = col.key;
        tableSortState.direction = 'asc';
      }
      renderRawData();
    });

    rawDom.thead.appendChild(th);
  });

  // Sort entries
  const sorted = entries.slice().sort((a, b) => {
    const col = tableSortState.column;
    const dir = tableSortState.direction === 'asc' ? 1 : -1;
    const va = a[col];
    const vb = b[col];

    if (col === 'keyType') {
      const ia = KEY_TYPE_ORDER.indexOf(va);
      const ib = KEY_TYPE_ORDER.indexOf(vb);
      if (ia >= 0 && ib >= 0) {
        const cmp = ia - ib;
        if (cmp !== 0) return cmp * dir;
      } else {
        const cmp2 = String(va).localeCompare(String(vb));
        if (cmp2 !== 0) return cmp2 * dir;
      }
      return String(a.metric).localeCompare(String(b.metric)) * dir;
    }

    if (col === 'metric') {
      const mc = String(va).localeCompare(String(vb));
      if (mc !== 0) return mc * dir;
      const ika = KEY_TYPE_ORDER.indexOf(a.keyType);
      const ikb = KEY_TYPE_ORDER.indexOf(b.keyType);
      return ((ika >= 0 ? ika : 99) - (ikb >= 0 ? ikb : 99)) * dir;
    }

    if (col === 'database' || col === 'scenario' || col === 'scale') {
      return String(va || '').localeCompare(String(vb || '')) * dir;
    }

    // Numeric sort
    if (va == null && vb == null) return 0;
    if (va == null) return 1;
    if (vb == null) return -1;
    return (Number(va) - Number(vb)) * dir;
  });

  // Compute best/worst per metric group
  const metricBestWorst = {};
  sorted.forEach(e => {
    if (e.median == null) return;
    if (!metricBestWorst[e.metric]) {
      metricBestWorst[e.metric] = { best: e.median, worst: e.median };
    }
    const mw = metricBestWorst[e.metric];
    const lowerBetter = LOWER_IS_BETTER.indexOf(e.metric) >= 0;
    if (lowerBetter) {
      if (e.median < mw.best) mw.best = e.median;
      if (e.median > mw.worst) mw.worst = e.median;
    } else {
      if (e.median > mw.best) mw.best = e.median;
      if (e.median < mw.worst) mw.worst = e.median;
    }
  });

  // Build rows
  rawDom.tbody.innerHTML = '';
  sorted.forEach(e => {
    const tr = document.createElement('tr');

    TABLE_COLUMNS.forEach(col => {
      const td = document.createElement('td');
      td.className = col.numeric ? 'num' : '';

      const val = e[col.key];
      if (col.key === 'keyType') {
        td.textContent = formatKeyTypeName(val);
      } else if (col.key === 'database') {
        td.textContent = formatDatabaseName(val);
      } else if (col.key === 'scenario') {
        td.textContent = formatScenarioName(val);
      } else if (col.key === 'scale') {
        td.textContent = val ? String(val).toUpperCase() : '\u2014';
      } else if (col.key === 'metric') {
        td.textContent = formatMetricName(val);
      } else if (col.key === 'cv') {
        td.textContent = val != null ? formatNumber(val) + '%' : '\u2014';
      } else {
        td.textContent = formatNumber(val);
      }

      // Conditional formatting on median
      if (col.key === 'median' && e.median != null && metricBestWorst[e.metric]) {
        const mw = metricBestWorst[e.metric];
        if (mw.best !== mw.worst) {
          if (e.median === mw.best) td.classList.add('cell-best');
          if (e.median === mw.worst) td.classList.add('cell-worst');
        }
      }

      tr.appendChild(td);
    });

    rawDom.tbody.appendChild(tr);
  });

  // Render mobile card layout
  renderRawCards(sorted, metricBestWorst);

  // Update footer
  if (rawDom.sortInfo) {
    rawDom.sortInfo.textContent = 'SORTED BY: ' +
      TABLE_COLUMNS.find(c => c.key === tableSortState.column)?.label.toUpperCase() +
      ' ' + (tableSortState.direction === 'asc' ? '\u25B4' : '\u25BE');
  }
  if (rawDom.count) {
    rawDom.count.textContent = sorted.length + ' ENTRIES';
  }
}

function renderRawCards(sorted, metricBestWorst) {
  if (!rawDom.cardsContainer) return;
  rawDom.cardsContainer.innerHTML = '';

  // Group entries by keyType + database + scenario + scale
  const groups = new Map();
  sorted.forEach(e => {
    const groupKey = e.keyType + '|' + e.database + '|' + e.scenario + '|' + e.scale;
    if (!groups.has(groupKey)) {
      groups.set(groupKey, { keyType: e.keyType, database: e.database, scenario: e.scenario, scale: e.scale, metrics: [] });
    }
    groups.get(groupKey).metrics.push(e);
  });

  // Build context parts that vary across cards (strip filtered dimensions)
  const contextKeys = ['database', 'scenario', 'scale'];
  const activeContextKeys = contextKeys.filter(k => rawFilterState[k] == null);

  groups.forEach(group => {
    const card = document.createElement('div');
    card.className = 'raw-card';

    // Header — line 1: swatch + key type name
    const header = document.createElement('div');
    header.className = 'raw-card-header';

    const swatch = document.createElement('span');
    swatch.className = 'raw-card-swatch';
    swatch.style.background = KEY_TYPE_COLORS[group.keyType] || '#999';
    header.appendChild(swatch);

    const title = document.createElement('span');
    title.className = 'raw-card-title';
    title.textContent = formatKeyTypeName(group.keyType);
    header.appendChild(title);

    // Header — line 2: unfiltered context dimensions (if any)
    const contextParts = [];
    activeContextKeys.forEach(k => {
      switch (k) {
        case 'database': contextParts.push(formatDatabaseName(group.database)); break;
        case 'scenario': contextParts.push(formatScenarioName(group.scenario)); break;
        case 'scale': contextParts.push(String(group.scale).toUpperCase()); break;
      }
    });

    if (contextParts.length > 0) {
      const context = document.createElement('span');
      context.className = 'raw-card-context';
      context.textContent = contextParts.join(' \u00b7 ');
      header.appendChild(context);
    }

    card.appendChild(header);

    // Body
    const body = document.createElement('div');
    body.className = 'raw-card-body';

    group.metrics.forEach(e => {
      const row = document.createElement('div');
      row.className = 'raw-card-row';

      const label = document.createElement('span');
      label.className = 'raw-card-label';
      label.textContent = formatMetricName(e.metric);
      row.appendChild(label);

      const value = document.createElement('span');
      value.className = 'raw-card-value';
      value.textContent = formatNumber(e.median);

      // Conditional formatting
      if (e.median != null && metricBestWorst[e.metric]) {
        const mw = metricBestWorst[e.metric];
        if (mw.best !== mw.worst) {
          if (e.median === mw.best) value.classList.add('cell-best');
          if (e.median === mw.worst) value.classList.add('cell-worst');
        }
      }

      row.appendChild(value);
      body.appendChild(row);
    });

    card.appendChild(body);
    rawDom.cardsContainer.appendChild(card);
  });
}
