// ==========================================================================
//  UUID Benchmark Dashboard — Shared Filter Modal (Bottom-Sheet)
//  Single module for the shared #filter-modal overlay used by Explorer
//  and Raw Data views on mobile.
// ==========================================================================

let bound = false;

/**
 * Opens the filter modal, relocating filter groups from their inline
 * position into the modal body.
 * @param {HTMLElement} sourceGroups — the .filter-modal-groups wrapper
 */
export function openFilterModal(sourceGroups) {
  const overlay = document.getElementById('filter-modal');
  const body = document.getElementById('filter-modal-body');
  if (!overlay || !body || !sourceGroups) return;

  // Guard against double-open (stacked listeners, fast taps)
  if (overlay.classList.contains('active')) return;

  // Move filter groups into the modal body
  body.innerHTML = '';
  const groups = sourceGroups.querySelectorAll('.filter-group');
  groups.forEach(g => body.appendChild(g));

  overlay.classList.add('active');
  document.body.style.overflow = 'hidden';

  // Store source so we can return groups on close
  overlay._sourceGroups = sourceGroups;

  // Update aria-expanded on the toggle that triggered this
  const toggle = sourceGroups.closest('.filter-bar')?.querySelector('.filter-toggle');
  if (toggle) toggle.setAttribute('aria-expanded', 'true');
}

/**
 * Closes the filter modal and returns filter groups to their inline location.
 */
export function closeFilterModal() {
  const overlay = document.getElementById('filter-modal');
  const body = document.getElementById('filter-modal-body');
  if (!overlay || !body) return;

  // Update aria-expanded before clearing _sourceGroups
  if (overlay._sourceGroups) {
    const toggle = overlay._sourceGroups.closest('.filter-bar')?.querySelector('.filter-toggle');
    if (toggle) toggle.setAttribute('aria-expanded', 'false');
  }

  overlay.classList.remove('active');
  document.body.style.overflow = '';

  // Return filter groups back to their inline location
  if (overlay._sourceGroups) {
    const groups = body.querySelectorAll('.filter-group');
    groups.forEach(g => overlay._sourceGroups.appendChild(g));
    overlay._sourceGroups = null;
  }
}

/**
 * Binds close handlers (backdrop click, close button, Escape key) once.
 * Safe to call multiple times — uses a guard to prevent double-binding.
 */
export function bindFilterModalClose() {
  if (bound) return;
  bound = true;

  const overlay = document.getElementById('filter-modal');
  if (!overlay) return;

  // Backdrop click
  overlay.addEventListener('click', (e) => {
    if (e.target === overlay) closeFilterModal();
  });

  // Close button
  const closeBtn = overlay.querySelector('.filter-modal-close');
  if (closeBtn) closeBtn.addEventListener('click', closeFilterModal);

  // Escape key
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && overlay.classList.contains('active')) {
      closeFilterModal();
    }
  });
}
