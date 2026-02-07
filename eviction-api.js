/**
 * Eviction Current-State API Client
 *
 * Fetches paginated records from the current-state endpoint,
 * deduplicates by docket_number (keeping the most recently updated row).
 *
 * stateData extraction is deferred — for now we return the top-level
 * row data as-is.
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

      // Stop conditions
      if (Array.isArray(result)) {
        if (result.length < PER_PAGE) break;
      } else {
        if (result.nextPage == null || items.length === 0) break;
        if (result.pageTotal && page >= result.pageTotal) break;
      }

      page++;
    }

    return allRows;
  }

  // ---------------------------------------------------------------------------
  // Deduplicate rows by docket_number — keep the most recently updated row
  // ---------------------------------------------------------------------------
  function dedupeByDocket(rows) {
    const map = new Map();

    for (const row of rows) {
      const key = normalizeDocket(row.docket_number);
      if (!key) continue;

      const existing = map.get(key);
      if (!existing) {
        map.set(key, row);
      } else {
        // Keep whichever row was updated more recently
        const rowTs = row.updated || row.created_at || 0;
        const existingTs = existing.updated || existing.created_at || 0;
        if (rowTs > existingTs) {
          map.set(key, row);
        }
      }
    }

    return Array.from(map.values());
  }

  function normalizeDocket(d) {
    if (!d) return '';
    return String(d).trim().replace(/\s+/g, ' ').toUpperCase();
  }

  // ---------------------------------------------------------------------------
  // Public: fetch all pages, dedupe, return raw rows
  // ---------------------------------------------------------------------------
  async function fetchAll(onProgress) {
    const rows = await fetchAllPages(onProgress);
    return dedupeByDocket(rows);
  }

  return { fetchAll, fetchAllPages, fetchPage, dedupeByDocket, normalizeDocket };
})();

if (typeof module !== 'undefined' && module.exports) {
  module.exports = EvictionAPI;
}
