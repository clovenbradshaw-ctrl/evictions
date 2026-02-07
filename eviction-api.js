/**
 * Eviction Current-State API Client
 *
 * Fetches paginated records from the current-state endpoint,
 * replays stateData entries to produce flat case records,
 * and returns them for downstream deduplication in index.html.
 */
const EvictionAPI = (function () {
  'use strict';

  const API_URL =
    'https://xvkq-pq7i-idtl.n7d.xano.io/api:3CsVHkZK/eviction_current_state';

  const PER_PAGE = 2500;
  const MAX_PAGES = 500; // safety limit

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
  // Replay stateData entries for a single API row → flat case object
  //
  // The eviction_current_state table stores each case as:
  //   { id, docket_number, fileDate, stateData: [...snapshots], updated }
  //
  // stateData is a JSON array of cumulative state snapshots.
  // We merge them in order (later entries win for non-empty values)
  // to produce the current state with all fields at the top level.
  // ---------------------------------------------------------------------------
  function replayRow(row) {
    // If EOMigration is loaded, delegate to its robust implementation
    if (typeof EOMigration !== 'undefined' && EOMigration.fromCurrentStateFormat) {
      return EOMigration.fromCurrentStateFormat(row);
    }

    // Inline fallback: replay stateData manually
    const docket = row.docket_number || row.object_id || '';

    let entries = row.stateData !== undefined ? row.stateData : row.data;
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

    // Ensure identity fields
    state.Docket_Number = state.Docket_Number || docket;
    if (row.fileDate && !state.File_Date) state.File_Date = row.fileDate;
    state._current_state_id = row.id;
    const updatedTs = row.updated || row.updated_at;
    state._last_updated = updatedTs
      ? (typeof updatedTs === 'number' ? updatedTs : new Date(updatedTs).getTime())
      : Date.now();

    return state;
  }

  // ---------------------------------------------------------------------------
  // Fetch all pages sequentially
  // ---------------------------------------------------------------------------
  async function fetchAllPages(onProgress) {
    const allRows = [];
    let page = 1;

    while (page <= MAX_PAGES) {
      const result = await fetchPage(page);

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

      // Log pagination metadata on first page for diagnostics
      if (page === 1 && !Array.isArray(result)) {
        console.log('API pagination:', {
          itemsTotal: result.itemsTotal,
          pageTotal: result.pageTotal,
          perPage: result.perPage,
          nextPage: result.nextPage,
          itemsOnPage: items.length
        });
      }

      // Stop conditions
      if (Array.isArray(result)) {
        if (result.length < PER_PAGE || result.length === 0) break;
        page++;
      } else {
        // Xano envelope
        if (items.length === 0) break;
        if (result.pageTotal && page >= result.pageTotal) break;
        if (result.itemsTotal && allRows.length >= result.itemsTotal) break;
        if (result.nextPage == null) break;
        // Use nextPage from the API response to advance
        page = result.nextPage;
      }
    }

    if (page >= MAX_PAGES) {
      console.warn('EvictionAPI: hit MAX_PAGES safety limit (' + MAX_PAGES + ')');
    }

    return allRows;
  }

  // ---------------------------------------------------------------------------
  // Public: fetch all pages, replay stateData, return flat case records
  // ---------------------------------------------------------------------------
  async function fetchAll(onProgress) {
    const rows = await fetchAllPages(onProgress);
    console.log('EvictionAPI: fetched', rows.length, 'raw rows from API');

    // Replay stateData to flatten each row into a case record
    const records = rows.map(replayRow);
    console.log('EvictionAPI: replayed stateData →', records.length, 'records');

    return records;
  }

  return { fetchAll, fetchAllPages, fetchPage, replayRow };
})();

if (typeof module !== 'undefined' && module.exports) {
  module.exports = EvictionAPI;
}
