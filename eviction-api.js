/**
 * Eviction Current-State API Client
 *
 * Simple fetch + replay + dedupe from:
 *   GET /eviction_current_state
 *
 * Each row has a stateData[] array of cumulative snapshots.
 * We replay those entries to build the latest state, then
 * deduplicate across rows by docket_number.
 */
const EvictionAPI = (function () {
  'use strict';

  const API_URL =
    'https://xvkq-pq7i-idtl.n7d.xano.io/api:3CsVHkZK/eviction_current_state';

  const PER_PAGE = 2500;
  const MAX_PAGES = 100; // safety limit

  // ---------------------------------------------------------------------------
  // Fetch a single page
  // ---------------------------------------------------------------------------
  async function fetchPage(page) {
    const url = `${API_URL}?page=${page}&per_page=${PER_PAGE}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`API error ${res.status}`);
    return res.json();
  }

  // ---------------------------------------------------------------------------
  // Fetch all pages
  // ---------------------------------------------------------------------------
  async function fetchAllPages(onProgress) {
    const allRows = [];
    let page = 1;

    while (page <= MAX_PAGES) {
      const result = await fetchPage(page);

      // Xano returns { items, curPage, nextPage, itemsReceived, ... }
      const items = Array.isArray(result) ? result : (result.items || []);
      allRows.push(...items);

      if (onProgress) {
        onProgress({
          page,
          pageTotal: result.pageTotal || null,
          fetched: allRows.length,
          total: result.itemsTotal || null,
        });
      }

      // Stop when no next page or empty response
      if (Array.isArray(result)) {
        if (result.length < PER_PAGE) break;
      } else {
        if (result.nextPage == null || items.length === 0) break;
      }

      page++;
    }

    return allRows;
  }

  // ---------------------------------------------------------------------------
  // Replay stateData entries for a single row → flat object
  // ---------------------------------------------------------------------------
  function replayRow(row) {
    const docket = row.docket_number || '';

    let entries = row.stateData;
    if (typeof entries === 'string') {
      try { entries = JSON.parse(entries); } catch { entries = []; }
    }
    if (!Array.isArray(entries)) entries = entries ? [entries] : [];

    // Cumulative merge — later entries win, but empty values never overwrite
    const state = {};
    for (const entry of entries) {
      if (!entry || typeof entry !== 'object') continue;
      for (const [key, value] of Object.entries(entry)) {
        if (value == null || value === '') continue;
        if (Array.isArray(value) && value.length === 0) continue;
        state[key] = value;
      }
    }

    // Ensure identity fields
    state.Docket_Number = state.Docket_Number || docket;

    // Carry over top-level metadata
    if (row.fileDate && !state.File_Date) state.File_Date = row.fileDate;
    state._current_state_id = row.id;
    state._updated = row.updated || row.created_at;

    return state;
  }

  // ---------------------------------------------------------------------------
  // Deduplicate by docket_number  (last-write-wins per field)
  // ---------------------------------------------------------------------------
  function dedupeByDocket(states) {
    const map = new Map();

    for (const state of states) {
      const key = normalizeDocket(state.Docket_Number);
      if (!key) continue;

      const existing = map.get(key);
      if (!existing) {
        state.Docket_Number = key;
        map.set(key, state);
      } else {
        // Merge: keep newer _updated as base, fill gaps from the other
        const newer =
          (state._updated || 0) >= (existing._updated || 0) ? state : existing;
        const older = newer === state ? existing : state;

        for (const [k, v] of Object.entries(older)) {
          if (newer[k] == null || newer[k] === '') {
            newer[k] = v;
          }
        }
        newer.Docket_Number = key;
        map.set(key, newer);
      }
    }

    return Array.from(map.values());
  }

  function normalizeDocket(d) {
    if (!d) return '';
    return String(d).trim().replace(/\s+/g, ' ').toUpperCase();
  }

  // ---------------------------------------------------------------------------
  // Public: fetch everything, replay, dedupe → clean array
  // ---------------------------------------------------------------------------
  async function fetchAll(onProgress) {
    const rows = await fetchAllPages(onProgress);
    const states = rows.map(replayRow);
    return dedupeByDocket(states);
  }

  return { fetchAll, fetchAllPages, fetchPage, replayRow, dedupeByDocket, normalizeDocket };
})();

if (typeof module !== 'undefined' && module.exports) {
  module.exports = EvictionAPI;
}
