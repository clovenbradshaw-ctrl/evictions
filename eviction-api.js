/**
 * Eviction Current-State API Client
 *
 * Fetches paginated records from the current-state endpoint.
 * Returns raw rows — deduplication is handled downstream in index.html
 * after normalizeRecord extracts docket numbers from stateData.
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
  // Public: fetch all pages and return raw rows
  // ---------------------------------------------------------------------------
  async function fetchAll(onProgress) {
    return fetchAllPages(onProgress);
  }

  return { fetchAll, fetchAllPages, fetchPage };
})();

if (typeof module !== 'undefined' && module.exports) {
  module.exports = EvictionAPI;
}
