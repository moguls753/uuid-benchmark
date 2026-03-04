// ==========================================================================
//  UUID Benchmark Dashboard — Annotation / Finding Navigation
// ==========================================================================

import { annotations, filterState } from './data.js';

let currentFindingIndex = -1;
let allFindingKeys = [];
let currentViewMode = 'cross-uuid';

// --- Build annotation key from current filter state ---
export function buildAnnotationKey(viewMode) {
  let keyOrder;
  if (viewMode === 'cross-uuid') {
    keyOrder = ['database', 'scenario', 'scale', 'connections', 'metric'];
  } else if (viewMode === 'cross-db') {
    keyOrder = ['scenario', 'scale', 'connections', 'metric'];
  } else if (viewMode === 'scale') {
    keyOrder = ['database', 'scenario', 'connections', 'metric'];
  } else {
    return null;
  }

  const parts = [];
  keyOrder.forEach(k => {
    if (filterState[k] != null) parts.push(String(filterState[k]));
  });
  return parts.join('|');
}

// --- Get annotation for current filter state ---
export function getAnnotation(viewMode) {
  const tabAnnotations = annotations[viewMode];
  if (!tabAnnotations) return null;
  const key = buildAnnotationKey(viewMode);
  return key ? tabAnnotations[key] || null : null;
}

// --- Get all annotation keys for a view mode ---
export function getAllAnnotationKeys(viewMode) {
  const tabAnnotations = annotations[viewMode];
  if (!tabAnnotations) return [];
  return Object.keys(tabAnnotations);
}

// --- Init finding navigation for a view mode ---
export function initFindingNavigation(viewMode) {
  currentViewMode = viewMode;
  allFindingKeys = getAllAnnotationKeys(viewMode);
  const currentKey = buildAnnotationKey(viewMode);
  currentFindingIndex = allFindingKeys.indexOf(currentKey);
  if (currentFindingIndex === -1 && allFindingKeys.length > 0) {
    currentFindingIndex = 0;
  }
}

// --- Navigate to next finding ---
export function nextFinding() {
  if (allFindingKeys.length === 0) return null;
  currentFindingIndex = (currentFindingIndex + 1) % allFindingKeys.length;
  return parseFindingKey(currentViewMode, allFindingKeys[currentFindingIndex]);
}

// --- Navigate to prev finding ---
export function prevFinding() {
  if (allFindingKeys.length === 0) return null;
  currentFindingIndex = (currentFindingIndex - 1 + allFindingKeys.length) % allFindingKeys.length;
  return parseFindingKey(currentViewMode, allFindingKeys[currentFindingIndex]);
}

// --- Progress state ---
export function getFindingProgress() {
  if (allFindingKeys.length === 0) return { current: 0, total: 0 };
  return { current: currentFindingIndex + 1, total: allFindingKeys.length };
}

// --- Get current finding content ---
export function getCurrentFinding() {
  if (allFindingKeys.length === 0 || currentFindingIndex < 0) return null;
  const key = allFindingKeys[currentFindingIndex];
  const tabAnnotations = annotations[currentViewMode];
  return tabAnnotations ? tabAnnotations[key] || null : null;
}

// --- Parse annotation key back into filter values ---
function parseFindingKey(viewMode, key) {
  const parts = key.split('|');
  const result = {};

  if (viewMode === 'cross-uuid') {
    // database|scenario|scale|connections|metric
    if (parts[0]) result.database = parts[0];
    if (parts[1]) result.scenario = parts[1];
    if (parts[2]) result.scale = parts[2];
    if (parts[3]) result.connections = Number(parts[3]);
    if (parts[4]) result.metric = parts[4];
  } else if (viewMode === 'cross-db') {
    // scenario|scale|connections|metric
    if (parts[0]) result.scenario = parts[0];
    if (parts[1]) result.scale = parts[1];
    if (parts[2]) result.connections = Number(parts[2]);
    if (parts[3]) result.metric = parts[3];
  } else if (viewMode === 'scale') {
    // database|scenario|connections|metric
    if (parts[0]) result.database = parts[0];
    if (parts[1]) result.scenario = parts[1];
    if (parts[2]) result.connections = Number(parts[2]);
    if (parts[3]) result.metric = parts[3];
  }

  return result;
}

// --- Sync finding index to current filter state ---
export function syncToCurrentFilters(viewMode) {
  currentViewMode = viewMode;
  const currentKey = buildAnnotationKey(viewMode);
  const idx = allFindingKeys.indexOf(currentKey);
  if (idx >= 0) currentFindingIndex = idx;
}
