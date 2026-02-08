/**
 * Webhook API Client
 *
 * Fetches eviction data from the n8n webhook endpoint.
 * Used as the primary remote data source before falling back
 * to the paginated Xano API or CSV.
 */
const WebhookAPI = (function () {
  'use strict';

  const WEBHOOK_URL =
    'https://n8n.intelechia.com/webhooka6d30bde-23a0-412a-adbe-3eefe33940f8';

  const TIMEOUT_MS = 60000; // 60 seconds

  // ---------------------------------------------------------------------------
  // Fetch all records from the webhook
  // ---------------------------------------------------------------------------
  async function fetchAll(onProgress) {
    if (onProgress) {
      onProgress({ stage: 'connecting', message: 'Connecting to webhook...' });
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), TIMEOUT_MS);

    try {
      const res = await fetch(WEBHOOK_URL, {
        method: 'GET',
        signal: controller.signal,
        headers: { 'Accept': 'application/json' }
      });

      clearTimeout(timeoutId);

      if (!res.ok) {
        throw new Error(`Webhook returned ${res.status} ${res.statusText}`);
      }

      if (onProgress) {
        onProgress({ stage: 'parsing', message: 'Parsing webhook response...' });
      }

      const body = await res.json();

      // Handle various response shapes:
      //   - Array of records directly
      //   - { items: [...] } or { data: [...] } wrapper
      //   - { records: [...] } wrapper
      let rawRecords;
      if (Array.isArray(body)) {
        rawRecords = body;
      } else if (body && typeof body === 'object') {
        rawRecords = body.items || body.data || body.records || [];
        if (!Array.isArray(rawRecords)) {
          rawRecords = [body];
        }
      } else {
        throw new Error('Unexpected webhook response format');
      }

      if (rawRecords.length === 0) {
        throw new Error('Webhook returned no records');
      }

      if (onProgress) {
        onProgress({ stage: 'processing', message: `Processing ${rawRecords.length} records...` });
      }

      // If records have stateData, replay them through EvictionAPI
      const firstRecord = rawRecords[0];
      const needsReplay = firstRecord &&
        (firstRecord.stateData !== undefined || firstRecord.data !== undefined) &&
        firstRecord.docket_number !== undefined;

      let records;
      if (needsReplay && typeof EvictionAPI !== 'undefined' && EvictionAPI.replayRow) {
        console.log('WebhookAPI: records have stateData, replaying...');
        records = rawRecords.map(row => EvictionAPI.replayRow(row));
      } else {
        records = rawRecords;
      }

      console.log('WebhookAPI: fetched', records.length, 'records from webhook');
      return records;

    } catch (err) {
      clearTimeout(timeoutId);
      if (err.name === 'AbortError') {
        throw new Error('Webhook request timed out');
      }
      throw err;
    }
  }

  return { fetchAll, WEBHOOK_URL };
})();

if (typeof module !== 'undefined' && module.exports) {
  module.exports = WebhookAPI;
}
