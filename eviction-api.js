/**
 * Eviction Current-State API Client
 *
 * Fetches from GET /eviction_current_state (2500/page),
 * replays stateData[] per row, dedupes by docket_number as it goes,
 * and stops when a full page adds no new dockets.
 */
const EvictionAPI = (function () {
  'use strict';

  const API_URL =
    'https://xvkq-pq7i-idtl.n7d.xano.io/api:3CsVHkZK/eviction_current_state';

  const PER_PAGE = 2500;
  const MAX_PAGES = 100;

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
      return true; // new docket
    }

    // Merge: newer _updated wins as base, backfill empty fields from older
    const newer = (state._updated || 0) >= (existing._updated || 0) ? state : existing;
    const older = newer === state ? existing : state;
    for (const [k, v] of Object.entries(older)) {
      if (newer[k] == null || newer[k] === '') newer[k] = v;
    }
    newer.Docket_Number = key;
    map.set(key, newer);
    return false; // existing docket updated
  }

  // ---------------------------------------------------------------------------
  // Fetch all, replay, dedupe as we go, stop when no new dockets on a page
  // ---------------------------------------------------------------------------
  async function fetchAll(onProgress) {
    const map = new Map();
    let page = 1;

    while (page <= MAX_PAGES) {
      const result = await fetchPage(page);
      const items = Array.isArray(result) ? result : (result.items || []);

      if (items.length === 0) break;

      // Replay + dedupe this page
      let newOnPage = 0;
      for (const row of items) {
        const state = replayRow(row);
        if (mergeInto(map, state)) newOnPage++;
      }

      if (onProgress) {
        onProgress({ page, unique: map.size, newOnPage });
      }

      console.log(`Page ${page}: ${items.length} rows, ${newOnPage} new dockets, ${map.size} unique total`);

      // Stop when we've seen (nearly) all unique dockets
      const newRate = newOnPage / items.length;
      if (newRate < 0.01) break; // <1% new = we've collected them all
      if (Array.isArray(result) && result.length < PER_PAGE) break;
      if (!Array.isArray(result) && result.nextPage == null) break;

      page++;
    }

    return Array.from(map.values());
  }

  return { fetchAll, fetchPage, replayRow, normalizeDocket };
})();

if (typeof module !== 'undefined' && module.exports) {
  module.exports = EvictionAPI;
}
