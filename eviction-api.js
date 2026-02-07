/**
 * Eviction Current-State API Client
 *
 * Fetches from GET /eviction_current_state (2500/page),
 * replays stateData[] per row, dedupes by docket_number.
 *
 * API response: { items, itemsTotal, pageTotal, curPage, nextPage, perPage }
 */
const EvictionAPI = (function () {
  'use strict';

  const API_URL =
    'https://xvkq-pq7i-idtl.n7d.xano.io/api:3CsVHkZK/eviction_current_state';

  const PER_PAGE = 2500;

  // ---------------------------------------------------------------------------
  // Fetch one page
  // ---------------------------------------------------------------------------
  async function fetchPage(page) {
    const url = `${API_URL}?page=${page}&per_page=${PER_PAGE}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`API error ${res.status}`);
    return res.json();
  }

  // ---------------------------------------------------------------------------
  // Replay stateData entries for one row → flat object
  // ---------------------------------------------------------------------------
  function replayRow(row) {
    const docket = row.docket_number || '';

    let entries = row.stateData;
    if (typeof entries === 'string') {
      try { entries = JSON.parse(entries); } catch { entries = []; }
    }
    if (!Array.isArray(entries)) entries = entries ? [entries] : [];

    const state = {};
    for (const entry of entries) {
      if (!entry || typeof entry !== 'object') continue;
      for (const [key, value] of Object.entries(entry)) {
        if (value == null || value === '') continue;
        if (Array.isArray(value) && value.length === 0) continue;
        state[key] = value;
      }
    }

    state.Docket_Number = state.Docket_Number || docket;
    if (row.fileDate && !state.File_Date) state.File_Date = row.fileDate;
    state._current_state_id = row.id;
    state._updated = row.updated || row.created_at;

    return state;
  }

  function normalizeDocket(d) {
    if (!d) return '';
    return String(d).trim().replace(/\s+/g, ' ').toUpperCase();
  }

  // ---------------------------------------------------------------------------
  // Merge a state into the dedup map (newer wins, fill gaps from older)
  // ---------------------------------------------------------------------------
  function mergeInto(map, state) {
    const key = normalizeDocket(state.Docket_Number);
    if (!key) return false;

    const existing = map.get(key);
    if (!existing) {
      state.Docket_Number = key;
      map.set(key, state);
      return true;
    }

    const newer = (state._updated || 0) >= (existing._updated || 0) ? state : existing;
    const older = newer === state ? existing : state;
    for (const [k, v] of Object.entries(older)) {
      if (newer[k] == null || newer[k] === '') newer[k] = v;
    }
    newer.Docket_Number = key;
    map.set(key, newer);
    return false;
  }

  // ---------------------------------------------------------------------------
  // Fetch all pages, replay + dedupe as we go
  // ---------------------------------------------------------------------------
  async function fetchAll(onProgress) {
    const map = new Map();
    let page = 1;
    let pageTotal = null;
    let itemsTotal = null;

    while (true) {
      const result = await fetchPage(page);
      const items = result.items || [];

      // Grab totals from first response
      if (page === 1) {
        pageTotal = result.pageTotal;
        itemsTotal = result.itemsTotal;
      }

      if (items.length === 0) break;

      for (const row of items) {
        mergeInto(map, replayRow(row));
      }

      if (onProgress) {
        onProgress({ page, pageTotal, itemsTotal, unique: map.size });
      }

      // Stop at last page
      if (result.nextPage == null) break;
      if (pageTotal && page >= pageTotal) break;

      page++;
    }

    return Array.from(map.values());
  }

  return { fetchAll, fetchPage, replayRow, normalizeDocket };
})();

if (typeof module !== 'undefined' && module.exports) {
  module.exports = EvictionAPI;
}
