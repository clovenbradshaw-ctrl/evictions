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
  const MAX_RETRIES = 4;
  const BASE_DELAY_MS = 2000;
  const PAGE_DELAY_MS = 1000; // throttle between pages to avoid 503s

  // ---------------------------------------------------------------------------
  // Delay helper
  // ---------------------------------------------------------------------------
  function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  // ---------------------------------------------------------------------------
  // Fetch a single page (with retry + exponential backoff for transient errors)
  // ---------------------------------------------------------------------------
  async function fetchPage(page, perPage) {
    perPage = perPage || PER_PAGE;
    const url = `${API_URL}?page=${page}&per_page=${perPage}`;

    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      try {
        const res = await fetch(url);
        if (res.ok) return res.json();

        // Retry on transient server errors (429, 500, 502, 503, 504)
        if ([429, 500, 502, 503, 504].includes(res.status) && attempt < MAX_RETRIES) {
          const wait = BASE_DELAY_MS * Math.pow(2, attempt);
          console.warn(`EvictionAPI: page ${page} returned ${res.status}, retrying in ${wait}ms (attempt ${attempt + 1}/${MAX_RETRIES})`);
          await delay(wait);
          continue;
        }

        throw new Error(`API error ${res.status}`);
      } catch (err) {
        // Network failures (CORS errors surface as TypeError: Failed to fetch)
        if (attempt < MAX_RETRIES && (err.name === 'TypeError' || err.message.includes('Failed to fetch'))) {
          const wait = BASE_DELAY_MS * Math.pow(2, attempt);
          console.warn(`EvictionAPI: page ${page} network error, retrying in ${wait}ms (attempt ${attempt + 1}/${MAX_RETRIES}): ${err.message}`);
          await delay(wait);
          continue;
        }
        throw err;
      }
    }
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
  //
  // Handles two Xano response shapes:
  //   1. Paginated envelope: { items, itemsTotal, pageTotal, curPage, nextPage, … }
  //   2. Plain array (no pagination metadata)
  //
  // When the Xano endpoint does not declare page/per_page as inputs the
  // pagination params are ignored and every request returns page 1.  We detect
  // this via curPage and fall back to a single large-page fetch so all records
  // are still retrieved.
  // ---------------------------------------------------------------------------
  async function fetchAllPages(onProgress) {
    const allRows = [];
    let page = 1;
    let expectedTotal = null;

    while (page <= MAX_PAGES) {
      let result;
      try {
        result = await fetchPage(page);
      } catch (err) {
        // If we already have data, log the error and return what we have
        if (allRows.length > 0) {
          console.warn(`EvictionAPI: page ${page} failed after retries, returning ${allRows.length} records already fetched. Error: ${err.message}`);
          if (onProgress) {
            onProgress({
              page,
              pageTotal: expectedTotal,
              fetched: allRows.length,
              total: expectedTotal ? expectedTotal * PER_PAGE : null,
              partial: true,
            });
          }
          break;
        }
        // No data fetched yet — rethrow so caller sees the failure
        throw err;
      }

      const items = Array.isArray(result) ? result : (result.items || []);
      const isEnvelope = !Array.isArray(result);

      // ---- Stuck-pagination detection ----
      // If Xano returned a curPage that doesn't match what we requested the
      // endpoint isn't accepting page/per_page inputs.  Rather than looping
      // and collecting duplicates, try one large-page fetch to get everything.
      if (isEnvelope && page > 1 && result.curPage != null && result.curPage !== page) {
        const totalItems = result.itemsTotal || 0;
        console.warn(
          `EvictionAPI: requested page ${page} but received curPage ${result.curPage}. ` +
          `Xano endpoint may not declare page/per_page inputs. ` +
          `Attempting single large-page fetch for all ${totalItems} records.`
        );

        if (totalItems > allRows.length) {
          // Re-fetch page 1 with a per_page large enough for all records
          try {
            const bigResult = await fetchPage(1, totalItems);
            const bigItems = Array.isArray(bigResult) ? bigResult : (bigResult.items || []);
            if (bigItems.length > allRows.length) {
              // Replace what we had with the larger result set
              allRows.length = 0;
              allRows.push(...bigItems);
              if (onProgress) {
                onProgress({
                  page: 1,
                  pageTotal: 1,
                  fetched: allRows.length,
                  total: totalItems,
                  pageItems: bigItems,
                });
              }
            }
          } catch (bigErr) {
            console.warn('EvictionAPI: large-page fallback failed, returning partial data:', bigErr.message);
          }
        }
        break;
      }

      allRows.push(...items);

      if (!expectedTotal && result.pageTotal) {
        expectedTotal = result.pageTotal;
      }

      if (onProgress) {
        onProgress({
          page,
          pageTotal: result.pageTotal || null,
          fetched: allRows.length,
          total: result.itemsTotal || null,
          pageItems: items,
        });
      }

      // Log pagination metadata on first page for diagnostics
      if (page === 1 && isEnvelope) {
        console.log('API pagination:', {
          itemsTotal: result.itemsTotal,
          pageTotal: result.pageTotal,
          perPage: result.perPage,
          nextPage: result.nextPage,
          curPage: result.curPage,
          itemsOnPage: items.length
        });
      }

      // Stop when we have all the records or got an empty page
      if (items.length === 0) break;
      if (result.itemsTotal && allRows.length >= result.itemsTotal) break;

      // Use nextPage as the canonical Xano stop signal
      if (isEnvelope && result.nextPage == null) break;

      // Throttle between pages to avoid overwhelming the API
      await delay(PAGE_DELAY_MS);
      page++;
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
