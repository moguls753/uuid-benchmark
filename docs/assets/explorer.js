// ==========================================================================
//  UUID Benchmark Dashboard — Explorer View
//  4-panel chart grid, sub-tabs, filter bar, expand/collapse, annotations.
// ==========================================================================

import {
  KEY_TYPE_COLORS, KEY_TYPE_ORDER, KEY_TYPE_SHORT, KEY_TYPE_LABELS,
  DATABASE_COLORS, DATABASE_ORDER, DATABASE_LABELS,
  EXPLORER_PANELS, PANEL_CONFIG, METRIC_GROUPS, METRIC_INFO, VIEW_FILTERS,
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
let panelMetricOverrides = [null, null, null, null];
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
  bindPanelMetricSelects();
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
      closeChartModal();
      panelMetricOverrides = [null, null, null, null];
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
      if (key === 'database') panelMetricOverrides = [null, null, null, null];
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

// --- Chart modal ---
let modalChart = null;
let modalOverlay = null;

function openChartModal(panelIndex) {
  if (!modalOverlay) modalOverlay = document.getElementById('chart-modal');
  if (!modalOverlay) return;

  const panel = dom.panels[panelIndex];
  if (!panel) return;

  const metrics = getPanelMetrics();
  const metric = metrics[panelIndex];
  if (!metric) return;

  const config = PANEL_CONFIG[metric];
  const title = modalOverlay.querySelector('.chart-modal-title');
  const unit = modalOverlay.querySelector('.chart-modal-unit');
  if (title) title.textContent = config ? config.label : metric.toUpperCase();
  if (unit) unit.textContent = config ? config.unit : '';

  // Build chart config for modal
  let chartConfig = null;
  if (activeMode === 'cross-uuid') {
    const entries = getEntriesForMetric('cross-uuid', metric);
    chartConfig = entries.length > 0 ? buildCrossUUIDChart(entries, metric) : null;
  } else if (activeMode === 'cross-db') {
    const savedMetric = filterState.metric;
    filterState.metric = metric;
    chartConfig = buildCrossDBChart(allEntries, filterState, metric);
    filterState.metric = savedMetric;
  } else if (activeMode === 'scale') {
    const entries = getEntriesForMetric('scale', metric);
    chartConfig = entries.length > 0 ? buildScaleChart(entries, metric) : null;
  }

  if (!chartConfig) return;

  // Disable entrance animation for modal chart
  chartConfig.options = chartConfig.options || {};
  chartConfig.options.animation = false;

  // Destroy previous modal chart
  if (modalChart) { modalChart.destroy(); modalChart = null; }

  const canvas = modalOverlay.querySelector('.chart-modal-body canvas');
  if (canvas) {
    modalChart = new Chart(canvas, {
      ...chartConfig,
      plugins: [errorBarPlugin],
    });
  }

  modalOverlay.classList.add('active');
  document.body.style.overflow = 'hidden';
}

function closeChartModal() {
  if (!modalOverlay) return;
  modalOverlay.classList.remove('active');
  document.body.style.overflow = '';
  if (modalChart) { modalChart.destroy(); modalChart = null; }
}

function bindPanelExpand() {
  dom.panels.forEach((panel, i) => {
    const btn = panel.querySelector('.panel-expand');
    if (!btn) return;
    btn.addEventListener('click', () => openChartModal(i));
  });

  // Close modal: backdrop click, close button, Escape key
  const overlay = document.getElementById('chart-modal');
  if (overlay) {
    overlay.addEventListener('click', (e) => {
      if (e.target === overlay) closeChartModal();
    });
    const closeBtn = overlay.querySelector('.chart-modal-close');
    if (closeBtn) closeBtn.addEventListener('click', closeChartModal);
  }
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') closeChartModal();
  });
}

// --- Auto-size select to fit selected option text ---
let _sizer = null;
function autoSizeSelect(select) {
  if (!_sizer) {
    _sizer = document.createElement('span');
    _sizer.style.cssText = 'position:absolute;visibility:hidden;white-space:nowrap;pointer-events:none';
    document.body.appendChild(_sizer);
  }
  const cs = getComputedStyle(select);
  _sizer.style.fontSize = cs.fontSize;
  _sizer.style.fontFamily = cs.fontFamily;
  _sizer.style.fontWeight = cs.fontWeight;
  _sizer.style.letterSpacing = cs.letterSpacing;
  _sizer.style.textTransform = cs.textTransform;
  _sizer.textContent = select.options[select.selectedIndex]?.text || '';
  select.style.width = (_sizer.offsetWidth + 16) + 'px'; // +16 for dropdown arrow
}

function resizeAllPanelSelects() {
  dom.panels.forEach(panel => {
    const select = panel.querySelector('.panel-metric-select');
    if (select) autoSizeSelect(select);
  });
}
let _resizeTimer;
window.addEventListener('resize', () => {
  clearTimeout(_resizeTimer);
  _resizeTimer = setTimeout(resizeAllPanelSelects, 150);
});

// --- Panel metric selects ---
function populatePanelSelects() {
  dom.panels.forEach((panel, i) => {
    const select = panel.querySelector('.panel-metric-select');
    if (!select) return;
    const currentVal = select.value;
    select.innerHTML = '';
    METRIC_GROUPS.forEach(group => {
      const optgroup = document.createElement('optgroup');
      optgroup.label = group.label;
      group.metrics.forEach(key => {
        if (!PANEL_CONFIG[key]) return;
        const opt = document.createElement('option');
        opt.value = key;
        opt.textContent = PANEL_CONFIG[key].label;
        optgroup.appendChild(opt);
      });
      select.appendChild(optgroup);
    });
    // Restore current selection
    if (currentVal) select.value = currentVal;
  });
}

function bindPanelMetricSelects() {
  dom.panels.forEach((panel, i) => {
    const select = panel.querySelector('.panel-metric-select');
    if (!select) return;
    select.addEventListener('change', () => {
      const defaults = getDefaultPanelMetrics();
      panelMetricOverrides[i] = select.value === defaults[i] ? null : select.value;
      // Destroy and re-render just this panel
      if (chartInstances[i]) { chartInstances[i].destroy(); chartInstances[i] = null; }
      renderPanel(i, select.value);
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
        ? `${progress.current} / ${progress.total}`
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
  closeChartModal();
  populatePanelSelects();

  const metrics = getPanelMetrics();
  let hasData = false;

  metrics.forEach((metric, i) => {
    const ok = renderPanel(i, metric);
    if (ok) hasData = true;
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

const BTREE_METRICS = ['page_splits', 'fragmentation', 'avg_leaf_density'];
const LSM_METRICS = ['sstable_count', 'bloom_filter_fp'];

function getMetricNAReason(metric) {
  const db = filterState.database;
  if (db === 'cassandra' && BTREE_METRICS.includes(metric)) return 'B-tree only';
  if (db !== 'cassandra' && LSM_METRICS.includes(metric)) return 'Cassandra only';
  return 'N/A';
}

function getDefaultPanelMetrics() {
  const db = filterState.database;
  if (activeMode === 'cross-uuid' && db === 'cassandra') return EXPLORER_PANELS.cassandra;
  if (activeMode === 'scale') {
    if (filterState.database === 'cassandra') return EXPLORER_PANELS.cassandra;
  }
  return EXPLORER_PANELS.default;
}

function getPanelMetrics() {
  const defaults = getDefaultPanelMetrics();
  return defaults.map((def, i) => panelMetricOverrides[i] || def);
}

function renderPanel(index, metric) {
  const panel = dom.panels[index];
  if (!panel) return false;

  const select = panel.querySelector('.panel-metric-select');
  const unit = panel.querySelector('.panel-unit');
  const config = PANEL_CONFIG[metric];

  if (select) { select.value = metric; autoSizeSelect(select); }
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
      // Show N/A state with context
      panel.classList.add('panel-empty');
      const reason = getMetricNAReason(metric);
      chartContainer.innerHTML = '<div class="panel-na"><span class="panel-na-dash">&mdash;</span>' + reason + '</div>';
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
        ? `${progress.current} / ${progress.total}`
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
          dom.annotationProgress.textContent = p.total > 0 ? `${p.current} / ${p.total}` : '';
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
          ? `${progress.current} / ${progress.total}`
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
