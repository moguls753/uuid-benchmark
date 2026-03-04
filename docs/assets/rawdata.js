// ==========================================================================
//  UUID Benchmark Dashboard — Raw Data Table View
//  Sortable columns, monochrome conditional formatting.
// ==========================================================================

import {
  KEY_TYPE_ORDER, LOWER_IS_BETTER,
  formatKeyTypeName, formatMetricName, formatNumber,
  formatDatabaseName, formatScenarioName,
} from './constants.js';
import { allEntries, coerceFilterValue, matchesPartial } from './data.js';

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

export function initRawData() {
  cacheRawDom();
  populateRawFilters();
  bindRawFilters();
  renderRawData();
}

export function destroyRawData() {
  // Clean up is minimal — just DOM
}

function cacheRawDom() {
  rawDom.filters = {
    database: document.getElementById('raw-filter-database'),
    scenario: document.getElementById('raw-filter-scenario'),
    scale: document.getElementById('raw-filter-scale'),
    connections: document.getElementById('raw-filter-connections'),
  };
  rawDom.thead = document.querySelector('#view-raw-data .data-table thead tr');
  rawDom.tbody = document.querySelector('#view-raw-data .data-table tbody');
  rawDom.sortInfo = document.querySelector('#view-raw-data .table-sort-info');
  rawDom.count = document.querySelector('#view-raw-data .table-count');
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
