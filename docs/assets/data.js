// ==========================================================================
//  UUID Benchmark Dashboard — Data Loading & Filter State
//  Owns all global state. Imported by every view module.
// ==========================================================================

import {
  KEY_TYPE_ORDER, DATABASE_ORDER, SCALE_ORDER, METRIC_GROUPS,
  CROSS_DB_NON_COMPARABLE, VIEW_FILTERS,
  formatDatabaseName, formatKeyTypeName, formatScenarioName, formatMetricName,
} from './constants.js';

// --- Global state ---
export let allEntries = [];
export let metadata = {};
export let annotations = {};

// --- Filter state ---
export const filterState = {
  database: null,
  keyType: null,
  scenario: null,
  scale: null,
  connections: null,
  metric: null,
};

// --- Data loading ---
export async function loadData() {
  const [dataRes, annoRes] = await Promise.all([
    fetch('data/data.json').then(r => {
      if (!r.ok) throw new Error('Failed to load data.json: ' + r.status);
      return r.json();
    }),
    fetch('data/annotations.json').then(r => {
      if (!r.ok) return {};
      return r.json();
    }).catch(() => ({})),
  ]);

  allEntries = dataRes.entries || [];
  metadata = dataRes.metadata || {};
  annotations = annoRes || {};
}

// --- Filter value coercion ---
export function coerceFilterValue(key, val) {
  if (key === 'connections') return Number(val);
  return val;
}

// --- Derive valid options for a filter given other visible filters ---
export function deriveValidOptions(key, visible, activeMode) {
  const partial = {};
  visible.forEach(k => {
    if (k !== key && filterState[k] != null) {
      partial[k] = filterState[k];
    }
  });

  const seen = {};
  allEntries.forEach(e => {
    if (matchesPartial(e, partial)) {
      const val = e[key];
      if (val != null) seen[val] = true;
    }
  });

  let opts = Object.keys(seen).map(v => coerceFilterValue(key, v));

  // Cross-DB: only show metrics comparable across databases with SEQUENTIAL baseline in 2+ DBs
  if (activeMode === 'cross-db' && key === 'metric') {
    opts = opts.filter(metric => {
      if (CROSS_DB_NON_COMPARABLE.indexOf(metric) >= 0) return false;
      const dbsWithBaseline = {};
      allEntries.forEach(e => {
        if (e.metric !== metric || e.keyType !== 'SEQUENTIAL') return;
        if (partial.scenario && e.scenario !== partial.scenario) return;
        if (partial.scale && e.scale !== partial.scale) return;
        if (partial.connections != null && String(e.connections) !== String(partial.connections)) return;
        dbsWithBaseline[e.database] = true;
      });
      return Object.keys(dbsWithBaseline).length >= 2;
    });
  }

  // Scale: only show options with data at 2+ scales
  if (activeMode === 'scale' && (key === 'connections' || key === 'database')) {
    opts = opts.filter(optVal => {
      const scales = {};
      allEntries.forEach(e => {
        if (String(e[key]) !== String(optVal)) return;
        let match = true;
        visible.forEach(k => {
          if (k === key) return;
          if (filterState[k] != null && String(e[k]) !== String(filterState[k])) {
            match = false;
          }
        });
        if (match) scales[e.scale] = true;
      });
      return Object.keys(scales).length >= 2;
    });
  }

  opts.sort((a, b) => sortComparator(key, a, b));
  return opts;
}

// --- Entry matching ---
export function matchesPartial(entry, partial) {
  const keys = Object.keys(partial);
  for (let i = 0; i < keys.length; i++) {
    if (String(entry[keys[i]]) !== String(partial[keys[i]])) return false;
  }
  return true;
}

// --- Sort comparator for filter options ---
export function sortComparator(key, a, b) {
  if (key === 'scale') {
    const scaleOrder = { '100k': 0, '1m': 1, '10m': 2, '100m': 3 };
    return (scaleOrder[a] || 0) - (scaleOrder[b] || 0);
  }
  if (key === 'connections') return Number(a) - Number(b);
  if (key === 'keyType') {
    const ia = KEY_TYPE_ORDER.indexOf(a);
    const ib = KEY_TYPE_ORDER.indexOf(b);
    if (ia >= 0 && ib >= 0) return ia - ib;
    return String(a).localeCompare(String(b));
  }
  if (key === 'database') {
    const da = DATABASE_ORDER.indexOf(a);
    const db2 = DATABASE_ORDER.indexOf(b);
    if (da >= 0 && db2 >= 0) return da - db2;
    return String(a).localeCompare(String(b));
  }
  return String(a).localeCompare(String(b));
}

// --- Format filter option for display ---
export function formatOption(key, val) {
  switch (key) {
    case 'database':    return formatDatabaseName(val);
    case 'keyType':     return formatKeyTypeName(val);
    case 'scenario':    return formatScenarioName(val);
    case 'metric':      return formatMetricName(val);
    case 'scale':       return String(val).toUpperCase();
    case 'connections': return val === 1 ? '1 connection' : val + ' connections';
    default:            return String(val);
  }
}

// --- Populate a single <select> element ---
export function fillSelect(key, opts, selectEl) {
  const prev = filterState[key];
  selectEl.innerHTML = '';

  if (key === 'metric' && opts.length > 0) {
    const optSet = {};
    opts.forEach(v => { optSet[v] = true; });

    METRIC_GROUPS.forEach(group => {
      const groupMetrics = group.metrics.filter(m => optSet[m]);
      if (groupMetrics.length === 0) return;
      const optgroup = document.createElement('optgroup');
      optgroup.label = group.label;
      groupMetrics.forEach(val => {
        const option = document.createElement('option');
        option.value = val;
        option.textContent = formatOption(key, val);
        optgroup.appendChild(option);
        delete optSet[val];
      });
      selectEl.appendChild(optgroup);
    });

    const remaining = Object.keys(optSet);
    remaining.forEach(val => {
      const option = document.createElement('option');
      option.value = val;
      option.textContent = formatOption(key, val);
      selectEl.appendChild(option);
    });
  } else {
    opts.forEach(val => {
      const option = document.createElement('option');
      option.value = val;
      option.textContent = formatOption(key, val);
      selectEl.appendChild(option);
    });
  }

  if (prev != null && opts.indexOf(coerceFilterValue(key, String(prev))) >= 0) {
    selectEl.value = String(prev);
    filterState[key] = prev;
  } else if (opts.length > 0) {
    let defaultVal = opts[0];
    if (key === 'metric' && opts.indexOf('throughput') >= 0) {
      defaultVal = 'throughput';
    }
    selectEl.value = String(defaultVal);
    filterState[key] = defaultVal;
  } else {
    filterState[key] = null;
  }
}

// --- Populate all visible filters ---
export function populateFilters(activeMode, domFilters) {
  const visible = VIEW_FILTERS[activeMode] || [];
  const validOptions = {};
  visible.forEach(key => {
    validOptions[key] = deriveValidOptions(key, visible, activeMode);
  });
  visible.forEach(key => {
    if (domFilters[key]) {
      fillSelect(key, validOptions[key], domFilters[key]);
    }
  });
}

// --- Cascade: recompute all filters except the one that changed ---
export function cascadeFilters(changedKey, activeMode, domFilters) {
  const visible = VIEW_FILTERS[activeMode] || [];
  visible.forEach(key => {
    if (key === changedKey) return;
    if (!domFilters[key]) return;
    const opts = deriveValidOptions(key, visible, activeMode);
    fillSelect(key, opts, domFilters[key]);
  });
}

// --- Get filtered entries for the active view mode ---
export function getFilteredEntries(activeMode) {
  const visible = VIEW_FILTERS[activeMode] || [];
  const partial = {};
  visible.forEach(k => {
    if (filterState[k] != null) partial[k] = filterState[k];
  });
  return allEntries.filter(e => matchesPartial(e, partial));
}

// --- Get entries for a specific metric (multi-panel grid) ---
export function getEntriesForMetric(activeMode, metric) {
  const visible = VIEW_FILTERS[activeMode] || [];
  const partial = {};
  visible.forEach(k => {
    if (filterState[k] != null) partial[k] = filterState[k];
  });
  partial.metric = metric;
  return allEntries.filter(e => matchesPartial(e, partial));
}
