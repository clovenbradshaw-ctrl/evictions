/**
 * Webhook API Client
 *
 * Fetches eviction data as a binary file from the n8n webhook endpoint.
 * Handles gzipped, raw JSON, and CSV binary payloads.
 * This is the sole data source for hydration.
 */
const WebhookAPI = (function () {
  'use strict';

  const WEBHOOK_URL =
    'https://n8n.intelechia.com/webhooka6d30bde-23a0-412a-adbe-3eefe33940f8';

  const TIMEOUT_MS = 120000; // 2 minutes for binary download

  // ---------------------------------------------------------------------------
  // Decompress gzipped ArrayBuffer using DecompressionStream (modern browsers)
  // Falls back to returning raw bytes if DecompressionStream is unavailable.
  // ---------------------------------------------------------------------------
  async function tryDecompress(buffer) {
    // Check for gzip magic bytes (1f 8b)
    const header = new Uint8Array(buffer.slice(0, 2));
    const isGzip = header[0] === 0x1f && header[1] === 0x8b;

    if (!isGzip) return buffer;

    if (typeof DecompressionStream === 'undefined') {
      console.warn('WebhookAPI: gzip detected but DecompressionStream not available');
      return buffer;
    }

    const ds = new DecompressionStream('gzip');
    const writer = ds.writable.getWriter();
    const reader = ds.readable.getReader();

    writer.write(new Uint8Array(buffer));
    writer.close();

    const chunks = [];
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
    }

    const totalLen = chunks.reduce((sum, c) => sum + c.byteLength, 0);
    const result = new Uint8Array(totalLen);
    let offset = 0;
    for (const chunk of chunks) {
      result.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return result.buffer;
  }

  // ---------------------------------------------------------------------------
  // Parse CSV text into an array of objects (header row → keys)
  // ---------------------------------------------------------------------------
  function parseCSV(text) {
    const lines = text.split(/\r?\n/).filter(l => l.trim());
    if (lines.length < 2) return [];

    // Handle quoted CSV fields
    function splitCSVLine(line) {
      const fields = [];
      let current = '';
      let inQuotes = false;
      for (let i = 0; i < line.length; i++) {
        const ch = line[i];
        if (ch === '"') {
          if (inQuotes && line[i + 1] === '"') {
            current += '"';
            i++;
          } else {
            inQuotes = !inQuotes;
          }
        } else if (ch === ',' && !inQuotes) {
          fields.push(current.trim());
          current = '';
        } else {
          current += ch;
        }
      }
      fields.push(current.trim());
      return fields;
    }

    const headers = splitCSVLine(lines[0]);
    const records = [];
    for (let i = 1; i < lines.length; i++) {
      const values = splitCSVLine(lines[i]);
      const obj = {};
      for (let j = 0; j < headers.length; j++) {
        obj[headers[j]] = values[j] !== undefined ? values[j] : '';
      }
      records.push(obj);
    }
    return records;
  }

  // ---------------------------------------------------------------------------
  // Detect format and parse the binary payload into records
  // ---------------------------------------------------------------------------
  function parsePayload(buffer, contentType) {
    const bytes = new Uint8Array(buffer);

    // Try to decode as text
    const text = new TextDecoder('utf-8').decode(bytes).trim();

    // 1. Try JSON (starts with [ or {)
    if (text[0] === '[' || text[0] === '{') {
      const body = JSON.parse(text);
      if (Array.isArray(body)) return body;
      if (body && typeof body === 'object') {
        const arr = body.items || body.data || body.records;
        if (Array.isArray(arr)) return arr;
        return [body];
      }
    }

    // 2. Try CSV (has comma-separated header row)
    if (contentType && contentType.includes('csv') || text.includes(',') && text.includes('\n')) {
      const records = parseCSV(text);
      if (records.length > 0) return records;
    }

    throw new Error('Could not parse webhook binary payload (not JSON or CSV)');
  }

  // ---------------------------------------------------------------------------
  // Fetch binary file from webhook, decompress if needed, parse into records
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
        signal: controller.signal
      });

      clearTimeout(timeoutId);

      if (!res.ok) {
        throw new Error(`Webhook returned ${res.status} ${res.statusText}`);
      }

      const contentType = res.headers.get('content-type') || '';
      const contentLength = res.headers.get('content-length');
      console.log('WebhookAPI: response content-type:', contentType,
                  'content-length:', contentLength || 'unknown');

      if (onProgress) {
        onProgress({ stage: 'downloading', message: 'Downloading data file...' });
      }

      // Read as binary ArrayBuffer
      const rawBuffer = await res.arrayBuffer();
      console.log('WebhookAPI: downloaded', rawBuffer.byteLength, 'bytes');

      if (onProgress) {
        onProgress({ stage: 'decompressing', message: 'Processing binary data...' });
      }

      // Decompress if gzipped
      const buffer = await tryDecompress(rawBuffer);

      if (onProgress) {
        onProgress({ stage: 'parsing', message: 'Parsing records...' });
      }

      // Parse into record array
      const rawRecords = parsePayload(buffer, contentType);
      console.log('WebhookAPI: parsed', rawRecords.length, 'raw records');

      if (rawRecords.length === 0) {
        throw new Error('Webhook returned no records');
      }

      // If records have stateData, replay them
      const first = rawRecords[0];
      const needsReplay = first &&
        (first.stateData !== undefined || first.data !== undefined) &&
        first.docket_number !== undefined;

      let records;
      if (needsReplay && typeof EvictionAPI !== 'undefined' && EvictionAPI.replayRow) {
        if (onProgress) {
          onProgress({ stage: 'replaying', message: `Replaying state for ${rawRecords.length} records...` });
        }
        records = rawRecords.map(row => EvictionAPI.replayRow(row));
      } else {
        records = rawRecords;
      }

      console.log('WebhookAPI: ready -', records.length, 'records');
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
