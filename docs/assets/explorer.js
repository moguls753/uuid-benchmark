// ==========================================================================
//  UUID Benchmark Dashboard — Explorer View
//  4-panel chart grid, sub-tabs, filter bar, expand/collapse, annotations.
// ==========================================================================

import {
  KEY_TYPE_COLORS, KEY_TYPE_ORDER, KEY_TYPE_SHORT, KEY_TYPE_LABELS,
  DATABASE_COLORS, DATABASE_ORDER, DATABASE_LABELS,
  EXPLORER_PANELS, PANEL_CONFIG, METRIC_INFO, VIEW_FILTERS,
  formatKeyTypeName, formatDatabaseName,
} from './constants.js';
import {
  filterState, allEntries, populateFilters, cascadeFilters,
  getEntriesForMetric, coerceFilterValue,
} from './data.js';
import { buildCrossUUIDChart, buildCrossDBChart, buildScaleChart, errorBarPlugin } from './charts.js';
import {
  initFindingNavigation, nextFinding, prevFinding,
  getFindingProgress, getAnnotation, getCurrentFinding,
  syncToCurrentFilters,
} from './annotations.js';

// --- State ---
let activeMode = 'cross-uuid';
let chartInstances = [null, null, null, null];
let expandedPanel = -1;
let mobileShowAll = false;
let skipAnnotationUpdate = false;

// --- DOM refs ---
let dom = {};

export function initExplorer(initialFilters, initialMode) {
  cacheDom();

  if (initialMode && ['cross-uuid', 'cross-db', 'scale'].includes(initialMode)) {
    activeMode = initialMode;
  }

  if (initialFilters) {
    Object.keys(initialFilters).forEach(k => {
      if (initialFilters[k] != null) filterState[k] = initialFilters[k];
    });
  }

  updateSubTabUI();
  updateFilterVisibility();
  populateFilters(activeMode, dom.filters);
  bindSubTabs();
  bindFilterEvents();
  bindPanelExpand();
  bindAnnotationNav();
  bindMobileToggle();
  initFindingNavigation(activeMode);
  renderExplorer();
}

export function destroyExplorer() {
  destroyCharts();
}

// --- DOM caching ---
function cacheDom() {
  dom.filters = {
    database: document.getElementById('filter-database'),
    keyType: document.getElementById('filter-keyType'),
    scenario: document.getElementById('filter-scenario'),
    scale: document.getElementById('filter-scale'),
    connections: document.getElementById('filter-connections'),
  };
  dom.panels = document.querySelectorAll('#view-explorer .chart-panel');
  dom.legend = document.getElementById('explorer-legend');
  dom.comparabilityWarning = document.querySelector('#view-explorer .comparability-warning');
  dom.comparabilityText = document.querySelector('#view-explorer .comparability-text');
  dom.annotationSection = document.querySelector('#view-explorer .annotation-section');
  dom.annotationFinding = document.querySelector('#view-explorer .annotation-finding');
  dom.annotationExplanation = document.querySelector('#view-explorer .annotation-explanation');
  dom.annotationProgress = document.querySelector('#view-explorer .annotation-progress');
  dom.noData = document.querySelector('#view-explorer .no-data');
  dom.chartGrid = document.querySelector('#view-explorer .chart-grid');
  dom.showAllBtn = document.querySelector('#view-explorer .show-all-metrics');
}

// --- Sub-tab switching ---
function bindSubTabs() {
  document.querySelectorAll('.explorer-tab').forEach(btn => {
    btn.addEventListener('click', () => {
      const mode = btn.dataset.mode;
      if (mode === activeMode) return;
      activeMode = mode;
      expandedPanel = -1;
      mobileShowAll = false;

      updateSubTabUI();
      updateFilterVisibility();
      populateFilters(activeMode, dom.filters);
      initFindingNavigation(activeMode);
      renderExplorer();
      if (window.updateURLHash) window.updateURLHash();
    });
  });
}

function updateSubTabUI() {
  document.querySelectorAll('.explorer-tab').forEach(btn => {
    const isActive = btn.dataset.mode === activeMode;
    btn.classList.toggle('active', isActive);
    btn.setAttribute('aria-selected', String(isActive));
  });
}

// --- Filter events ---
function bindFilterEvents() {
  Object.keys(dom.filters).forEach(key => {
    const el = dom.filters[key];
    if (!el) return;
    el.addEventListener('change', () => {
      filterState[key] = coerceFilterValue(key, el.value);
      cascadeFilters(key, activeMode, dom.filters);
      syncToCurrentFilters(activeMode);
      renderExplorer();
      if (window.updateURLHash) window.updateURLHash();
    });
  });
}

// --- Filter visibility per mode ---
function updateFilterVisibility() {
  const visible = VIEW_FILTERS[activeMode] || [];
  Object.keys(dom.filters).forEach(key => {
    const group = dom.filters[key]?.closest('.filter-group');
    if (group) group.hidden = visible.indexOf(key) < 0;
  });
}

// --- Panel expand/collapse ---
function bindPanelExpand() {
  dom.panels.forEach((panel, i) => {
    const btn = panel.querySelector('.panel-expand');
    if (!btn) return;
    btn.addEventListener('click', () => {
      if (expandedPanel === i) {
        expandedPanel = -1;
        panel.classList.remove('expanded');
        btn.innerHTML = '&#10530;';
        btn.setAttribute('aria-label', 'Expand panel');
      } else {
        // Collapse previous
        dom.panels.forEach((p, j) => {
          p.classList.remove('expanded');
          const b = p.querySelector('.panel-expand');
          if (b) { b.innerHTML = '&#10530;'; b.setAttribute('aria-label', 'Expand panel'); }
        });
        expandedPanel = i;
        panel.classList.add('expanded');
        btn.innerHTML = '&times;';
        btn.setAttribute('aria-label', 'Collapse panel');
      }
      // Destroy and re-render the affected chart for new size
      if (chartInstances[i]) { chartInstances[i].destroy(); chartInstances[i] = null; }
      const metrics = getPanelMetrics();
      if (metrics[i]) renderPanel(i, metrics[i]);
    });
  });
}

// --- Annotation navigation ---
function bindAnnotationNav() {
  const prevBtn = document.querySelector('#view-explorer .annotation-prev');
  const nextBtn = document.querySelector('#view-explorer .annotation-next');

  if (prevBtn) {
    prevBtn.addEventListener('click', () => {
      const filters = prevFinding();
      if (filters) applyFindingFilters(filters);
    });
  }
  if (nextBtn) {
    nextBtn.addEventListener('click', () => {
      const filters = nextFinding();
      if (filters) applyFindingFilters(filters);
    });
  }
}

function applyFindingFilters(filters) {
  // Apply non-metric filters to filterState (metric is panel-driven)
  Object.keys(filters).forEach(k => {
    if (k !== 'metric' && filters[k] != null) filterState[k] = filters[k];
  });
  populateFilters(activeMode, dom.filters);

  // Skip updateAnnotation inside renderExplorer — we handle it here
  skipAnnotationUpdate = true;
  renderExplorer();
  skipAnnotationUpdate = false;

  // Show the navigated finding directly (index was set by next/prevFinding)
  const finding = getCurrentFinding();
  const progress = getFindingProgress();
  if (finding && dom.annotationFinding) {
    dom.annotationFinding.textContent = finding.finding;
    dom.annotationExplanation.textContent = finding.explanation || '';
    dom.annotationExplanation.hidden = !finding.explanation;
    dom.annotationSection.hidden = false;
    if (dom.annotationProgress) {
      dom.annotationProgress.textContent = progress.total > 0
        ? `(${progress.current} / ${progress.total})`
        : '';
    }
  }

  if (window.updateURLHash) window.updateURLHash();
}

// --- Mobile toggle ---
function bindMobileToggle() {
  if (!dom.showAllBtn) return;
  dom.showAllBtn.addEventListener('click', () => {
    mobileShowAll = !mobileShowAll;
    updateMobileVisibility();
    dom.showAllBtn.textContent = mobileShowAll ? 'SHOW FEWER METRICS \u25C2' : 'SHOW ALL METRICS \u25B8';
  });
}

function updateMobileVisibility() {
  if (window.innerWidth >= 768) return;
  dom.panels.forEach((panel, i) => {
    if (i >= 2) {
      panel.style.display = mobileShowAll ? '' : 'none';
    }
  });
  if (dom.showAllBtn) dom.showAllBtn.hidden = false;
}

// --- Main render ---
function renderExplorer() {
  destroyCharts();
  expandedPanel = -1;

  const metrics = getPanelMetrics();
  let hasData = false;

  metrics.forEach((metric, i) => {
    const ok = renderPanel(i, metric);
    if (ok) hasData = true;
  });

  // Reset panel expand UI
  dom.panels.forEach(p => {
    p.classList.remove('expanded');
    const btn = p.querySelector('.panel-expand');
    if (btn) { btn.innerHTML = '&#10530;'; btn.setAttribute('aria-label', 'Expand panel'); }
  });

  updateLegend();
  updateComparabilityWarning();
  updateAnnotation();

  if (dom.noData) dom.noData.hidden = hasData;
  if (dom.chartGrid) dom.chartGrid.style.display = hasData ? '' : 'none';

  // Mobile: show/hide panels 3-4
  if (window.innerWidth < 768) {
    updateMobileVisibility();
  } else if (dom.showAllBtn) {
    dom.showAllBtn.hidden = true;
  }
}

function getPanelMetrics() {
  const db = filterState.database;
  if (activeMode === 'cross-uuid' && db === 'cassandra') return EXPLORER_PANELS.cassandra;
  if (activeMode === 'scale') {
    // For scale, check the selected database (if in cross-uuid/scale modes)
    const scaleDb = filterState.database;
    if (scaleDb === 'cassandra') return EXPLORER_PANELS.cassandra;
  }
  return EXPLORER_PANELS.default;
}

function renderPanel(index, metric) {
  const panel = dom.panels[index];
  if (!panel) return false;

  const label = panel.querySelector('.panel-label');
  const unit = panel.querySelector('.panel-unit');
  const canvas = panel.querySelector('canvas');
  const config = PANEL_CONFIG[metric];

  if (label) label.textContent = config ? config.label : metric.toUpperCase();
  if (unit) unit.textContent = config ? config.unit : '';

  let chartConfig = null;

  if (activeMode === 'cross-uuid') {
    const entries = getEntriesForMetric('cross-uuid', metric);
    chartConfig = entries.length > 0 ? buildCrossUUIDChart(entries, metric) : null;
  } else if (activeMode === 'cross-db') {
    // Cross-DB needs metric in filter state temporarily
    const savedMetric = filterState.metric;
    filterState.metric = metric;
    chartConfig = buildCrossDBChart(allEntries, filterState, metric);
    filterState.metric = savedMetric;
  } else if (activeMode === 'scale') {
    const entries = getEntriesForMetric('scale', metric);
    chartConfig = entries.length > 0 ? buildScaleChart(entries, metric) : null;
  }

  panel.classList.remove('panel-empty');

  // Clear previous canvas content
  const chartContainer = panel.querySelector('.panel-chart');
  if (chartContainer) {
    chartContainer.innerHTML = '';
    const newCanvas = document.createElement('canvas');
    chartContainer.appendChild(newCanvas);

    if (chartConfig) {
      chartInstances[index] = new Chart(newCanvas, {
        ...chartConfig,
        plugins: [errorBarPlugin],
      });
      return true;
    } else {
      // Show N/A state
      panel.classList.add('panel-empty');
      chartContainer.innerHTML = '<div class="panel-na"><span class="panel-na-dash">&mdash;</span>N/A</div>';
      return false;
    }
  }
  return false;
}

// --- Legend strip ---
function updateLegend() {
  if (!dom.legend) return;
  dom.legend.innerHTML = '';

  if (activeMode === 'cross-db') {
    // Show database colors
    DATABASE_ORDER.forEach(db => {
      const item = document.createElement('span');
      item.className = 'legend-item';
      const swatch = document.createElement('span');
      swatch.className = 'legend-swatch';
      swatch.style.background = DATABASE_COLORS[db] || '#999';
      item.appendChild(swatch);
      item.appendChild(document.createTextNode(DATABASE_LABELS[db] || db));
      dom.legend.appendChild(item);
    });
  } else {
    // Show key type colors
    KEY_TYPE_ORDER.forEach(kt => {
      // Only show key types that have data for current filters
      const item = document.createElement('span');
      item.className = 'legend-item';
      const swatch = document.createElement('span');
      swatch.className = 'legend-swatch';
      swatch.style.background = KEY_TYPE_COLORS[kt] || '#999';
      item.appendChild(swatch);
      item.appendChild(document.createTextNode(KEY_TYPE_SHORT[kt] || kt));
      dom.legend.appendChild(item);
    });
  }
}

// --- Comparability warning ---
function updateComparabilityWarning() {
  if (!dom.comparabilityWarning) return;

  if (activeMode !== 'cross-db') {
    dom.comparabilityWarning.hidden = true;
    return;
  }

  // Check if any panel metrics have comparability notes
  const metrics = getPanelMetrics();
  const warnings = [];
  metrics.forEach(metric => {
    const info = METRIC_INFO[metric];
    if (info && info.comparability) {
      warnings.push(info.comparability);
    }
  });

  if (warnings.length > 0) {
    dom.comparabilityText.textContent = warnings[0];
    dom.comparabilityWarning.hidden = false;
  } else {
    dom.comparabilityWarning.hidden = true;
  }
}

// --- Annotation display ---
function updateAnnotation() {
  if (!dom.annotationSection) return;
  if (skipAnnotationUpdate) return;

  // For multi-panel, we use the first panel's metric (throughput) as the annotation key metric
  const metrics = getPanelMetrics();
  const savedMetric = filterState.metric;
  filterState.metric = metrics[0]; // Use throughput for annotation lookup
  syncToCurrentFilters(activeMode);

  const annotation = getAnnotation(activeMode);
  const progress = getFindingProgress();

  if (annotation && annotation.finding) {
    dom.annotationFinding.textContent = annotation.finding;
    dom.annotationExplanation.textContent = annotation.explanation || '';
    dom.annotationExplanation.hidden = !annotation.explanation;
    dom.annotationSection.hidden = false;

    if (dom.annotationProgress) {
      dom.annotationProgress.textContent = progress.total > 0
        ? `(${progress.current} / ${progress.total})`
        : '';
    }
  } else {
    // Check if there's a finding for any of the panel metrics
    let found = false;
    for (let i = 1; i < metrics.length; i++) {
      filterState.metric = metrics[i];
      syncToCurrentFilters(activeMode);
      const ann = getAnnotation(activeMode);
      if (ann && ann.finding) {
        dom.annotationFinding.textContent = ann.finding;
        dom.annotationExplanation.textContent = ann.explanation || '';
        dom.annotationExplanation.hidden = !ann.explanation;
        dom.annotationSection.hidden = false;
        found = true;

        const p = getFindingProgress();
        if (dom.annotationProgress) {
          dom.annotationProgress.textContent = p.total > 0 ? `(${p.current} / ${p.total})` : '';
        }
        break;
      }
    }

    if (!found) {
      dom.annotationFinding.innerHTML = '<span class="annotation-none">No curated finding for this combination</span>';
      dom.annotationExplanation.textContent = '';
      dom.annotationExplanation.hidden = true;
      dom.annotationSection.hidden = false;

      if (dom.annotationProgress) {
        dom.annotationProgress.textContent = progress.total > 0
          ? `(${progress.current} / ${progress.total})`
          : '';
      }
    }
  }

  filterState.metric = savedMetric;
}

// --- Chart cleanup ---
function destroyCharts() {
  chartInstances.forEach((c, i) => {
    if (c) { c.destroy(); chartInstances[i] = null; }
  });
}

// --- Public getters for URL state ---
export function getActiveMode() {
  return activeMode;
}
