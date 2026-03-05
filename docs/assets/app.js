// ==========================================================================
//  UUID Benchmark Dashboard — Entry Point & Router
//  Data loading, view switching, URL hash state management.
// ==========================================================================

import { loadData, filterState } from './data.js';
import { initSummary, destroySummary } from './summary.js';
import { initExplorer, destroyExplorer, getActiveMode } from './explorer.js';
import { initRawData, destroyRawData } from './rawdata.js';
import { bindFilterModalClose } from './filterModal.js';

let activeView = 'summary';

document.addEventListener('DOMContentLoaded', async () => {
  try {
    await loadData();
    bindFilterModalClose();
    parseURLHash();
    initActiveView();
    bindMainNav();
    bindHashChange();
  } catch (err) {
    console.error('Failed to load data:', err);
  }
});

// --- Main navigation ---
function bindMainNav() {
  document.querySelectorAll('.nav-tab').forEach(btn => {
    btn.addEventListener('click', () => {
      const view = btn.dataset.view;
      if (view === activeView) return;
      switchView(view);
    });
  });
}

function switchView(view, initialFilters, initialMode) {
  destroyCurrentView();

  document.querySelectorAll('.nav-tab').forEach(btn => {
    const isActive = btn.dataset.view === view;
    btn.classList.toggle('active', isActive);
    btn.setAttribute('aria-selected', String(isActive));
    const label = btn.dataset.view.toUpperCase().replace('-', ' ');
    btn.textContent = label;
  });

  document.querySelectorAll('.view-panel').forEach(panel => {
    panel.hidden = panel.id !== `view-${view}`;
  });

  activeView = view;
  initActiveView(initialFilters, initialMode);
  updateURLHashInternal();
}

function initActiveView(initialFilters, initialMode) {
  switch (activeView) {
    case 'summary':
      initSummary();
      break;
    case 'explorer':
      initExplorer(initialFilters, initialMode || parsedMode);
      break;
    case 'raw-data':
      initRawData();
      break;
  }
}

function destroyCurrentView() {
  switch (activeView) {
    case 'summary':  destroySummary();  break;
    case 'explorer': destroyExplorer(); break;
    case 'raw-data': destroyRawData();  break;
  }
}

// --- URL Hash State ---
let parsedMode = null; // Explorer sub-tab mode from URL hash

function parseURLHash() {
  const hash = window.location.hash.replace('#', '');
  if (!hash) {
    activeView = 'summary';
    parsedMode = null;
    return;
  }

  const params = {};
  hash.split('&').forEach(pair => {
    const [key, val] = pair.split('=');
    if (key && val) params[decodeURIComponent(key)] = decodeURIComponent(val);
  });

  if (params.view) {
    const validViews = ['summary', 'explorer', 'raw-data'];
    activeView = validViews.includes(params.view) ? params.view : 'summary';
  }

  // Restore explorer sub-tab mode
  if (params.mode) {
    const validModes = ['cross-uuid', 'cross-db', 'scale'];
    parsedMode = validModes.includes(params.mode) ? params.mode : null;
  } else {
    parsedMode = null;
  }

  // Restore filter state
  if (params.db) filterState.database = params.db;
  if (params.key) filterState.keyType = params.key;
  if (params.scenario) filterState.scenario = params.scenario;
  if (params.scale) filterState.scale = params.scale;
  if (params.conn) filterState.connections = Number(params.conn);

  // Show correct view
  document.querySelectorAll('.nav-tab').forEach(btn => {
    const isActive = btn.dataset.view === activeView;
    btn.classList.toggle('active', isActive);
    btn.setAttribute('aria-selected', String(isActive));
    const label = btn.dataset.view.toUpperCase().replace('-', ' ');
    btn.textContent = label;
  });

  document.querySelectorAll('.view-panel').forEach(panel => {
    panel.hidden = panel.id !== `view-${activeView}`;
  });
}

function updateURLHashInternal() {
  const parts = [`view=${activeView}`];

  if (activeView === 'explorer') {
    const mode = getActiveMode();
    parts.push(`mode=${mode}`);

    if (filterState.database) parts.push(`db=${filterState.database}`);
    if (filterState.keyType) parts.push(`key=${filterState.keyType}`);
    if (filterState.scenario) parts.push(`scenario=${filterState.scenario}`);
    if (filterState.scale) parts.push(`scale=${filterState.scale}`);
    if (filterState.connections != null) parts.push(`conn=${filterState.connections}`);
  }

  const hash = '#' + parts.join('&');
  if (window.location.hash !== hash) {
    history.replaceState(null, '', hash);
  }
}

// Expose for explorer.js to call
window.updateURLHash = updateURLHashInternal;

function bindHashChange() {
  window.addEventListener('hashchange', () => {
    const prevView = activeView;
    // Destroy the CURRENT (previous) view before parseURLHash changes activeView
    destroyCurrentView();
    parseURLHash();
    if (prevView !== activeView) {
      initActiveView();
    }
  });
}

// --- Cross-view navigation (called from summary.js) ---
window.navigateToExplorer = function (filters, mode) {
  switchView('explorer', filters, mode);
};
