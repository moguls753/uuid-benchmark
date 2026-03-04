// ==========================================================================
//  UUID Benchmark Dashboard — Summary View
//  KPI cards, database cards, UUID legend, methodology toggle.
// ==========================================================================

import {
  KEY_TYPE_COLORS, KEY_TYPE_ORDER, KEY_TYPE_LABELS, KEY_TYPE_SHORT,
} from './constants.js';

export function initSummary() {
  renderLegend();
  bindExploreButtons();
  bindKPICards();
  bindMethodologyToggle();
}

export function destroySummary() {
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
