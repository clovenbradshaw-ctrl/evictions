/**
 * EO Integration Helper
 *
 * Bridges the EO event-sourced system with the eviction tracker application.
 * Provides a clean API for:
 * - Syncing data from the EO operations table
 * - Creating new events for case changes
 * - Transitioning from legacy to EO-based operations
 */

const EOIntegration = (function() {
  'use strict';

  // Requires EOMigration to be loaded first
  if (typeof EOMigration === 'undefined') {
    console.error('EOIntegration requires EOMigration to be loaded first');
    return null;
  }

  // =============================================================================
  // CONFIGURATION
  // =============================================================================

  const CONFIG = {
    // Local storage keys for EO state
    LAST_SYNC_KEY: 'eo_last_sync_ts',
    EVENTS_CACHE_KEY: 'eo_events_cache',
    STATE_CACHE_KEY: 'eo_state_cache',

    // IndexedDB settings
    DB_NAME: 'eoEventsDB',
    DB_VERSION: 1,
    STORE_NAME: 'events',

    // Sync settings
    SYNC_INTERVAL_MS: 5 * 60 * 1000, // 5 minutes
    MAX_EVENTS_IN_MEMORY: 50000,

    // Feature flags
    EO_ENABLED: true
  };

  // =============================================================================
  // INDEXEDDB HELPER (for large event storage - localStorage has ~5MB limit)
  // =============================================================================

  const eventsDB = {
    async open() {
      return new Promise((resolve, reject) => {
        const request = indexedDB.open(CONFIG.DB_NAME, CONFIG.DB_VERSION);
        request.onerror = () => reject(request.error);
        request.onsuccess = () => resolve(request.result);
        request.onupgradeneeded = (event) => {
          const db = event.target.result;
          if (!db.objectStoreNames.contains(CONFIG.STORE_NAME)) {
            db.createObjectStore(CONFIG.STORE_NAME, { keyPath: 'id' });
          }
        };
      });
    },

    async save(events, timestamp) {
      try {
        const db = await this.open();
        return new Promise((resolve, reject) => {
          const tx = db.transaction(CONFIG.STORE_NAME, 'readwrite');
          const store = tx.objectStore(CONFIG.STORE_NAME);
          store.put({ id: 'eo_events', data: events, timestamp });
          tx.oncomplete = () => {
            db.close();
            console.log('💾 Saved', events.length, 'EO events to IndexedDB');
            resolve(true);
          };
          tx.onerror = () => {
            db.close();
            reject(tx.error);
          };
        });
      } catch (e) {
        console.error('IndexedDB save failed:', e);
        return false;
      }
    },

    async load() {
      try {
        const db = await this.open();
        return new Promise((resolve, reject) => {
          const tx = db.transaction(CONFIG.STORE_NAME, 'readonly');
          const store = tx.objectStore(CONFIG.STORE_NAME);
          const request = store.get('eo_events');
          request.onsuccess = () => {
            db.close();
            resolve(request.result || null);
          };
          request.onerror = () => {
            db.close();
            reject(request.error);
          };
        });
      } catch (e) {
        console.error('IndexedDB load failed:', e);
        return null;
      }
    },

    async clear() {
      try {
        const db = await this.open();
        return new Promise((resolve, reject) => {
          const tx = db.transaction(CONFIG.STORE_NAME, 'readwrite');
          const store = tx.objectStore(CONFIG.STORE_NAME);
          const request = store.clear();
          tx.oncomplete = () => {
            db.close();
            console.log('💾 Cleared EO events IndexedDB cache');
            resolve(true);
          };
          tx.onerror = () => {
            db.close();
            reject(tx.error);
          };
        });
      } catch (e) {
        console.error('IndexedDB clear failed:', e);
        return false;
      }
    }
  };

  // =============================================================================
  // STATE
  // =============================================================================

  let eventsCache = [];
  let stateCache = new Map();
  let lastSyncTimestamp = 0;
  let syncInterval = null;

  // =============================================================================
  // INITIALIZATION
  // =============================================================================

  /**
   * Initialize the EO integration
   */
  async function initialize() {
    console.log('🔧 Initializing EO Integration...');

    // Load last sync timestamp
    const storedTs = localStorage.getItem(CONFIG.LAST_SYNC_KEY);
    if (storedTs) {
      lastSyncTimestamp = parseInt(storedTs, 10);
      console.log(`  Last sync: ${new Date(lastSyncTimestamp).toISOString()}`);
    }

    // Try to load cached events from IndexedDB
    await loadEventsFromCache();

    // Start background sync
    if (CONFIG.EO_ENABLED) {
      startBackgroundSync();
    }

    console.log('✅ EO Integration initialized');
  }

  /**
   * Load events from local cache (IndexedDB with localStorage fallback)
   */
  async function loadEventsFromCache() {
    try {
      // Try IndexedDB first (preferred - no size limit)
      const idbResult = await eventsDB.load();
      if (idbResult && Array.isArray(idbResult.data) && idbResult.data.length > 0) {
        eventsCache = idbResult.data;
        console.log(`  Loaded ${eventsCache.length} cached events from IndexedDB`);
        rebuildStateCache();
        return;
      }

      // Fallback to localStorage (for backwards compatibility)
      const cached = localStorage.getItem(CONFIG.EVENTS_CACHE_KEY);
      if (cached) {
        eventsCache = JSON.parse(cached);
        console.log(`  Loaded ${eventsCache.length} cached events from localStorage (legacy)`);

        // Migrate to IndexedDB for future loads
        await saveEventsToCache();
        // Clear legacy localStorage cache after migration
        try {
          localStorage.removeItem(CONFIG.EVENTS_CACHE_KEY);
          console.log('  Migrated events cache from localStorage to IndexedDB');
        } catch (e) {
          // Ignore cleanup errors
        }

        rebuildStateCache();
      }
    } catch (e) {
      console.warn('Failed to load events cache:', e);
      eventsCache = [];
    }
  }

  /**
   * Save events to local cache (IndexedDB with localStorage fallback)
   */
  async function saveEventsToCache() {
    // Limit cache size
    if (eventsCache.length > CONFIG.MAX_EVENTS_IN_MEMORY) {
      // Keep most recent events
      eventsCache = eventsCache.slice(-CONFIG.MAX_EVENTS_IN_MEMORY);
    }

    // Try IndexedDB first (preferred - no size limit)
    try {
      const success = await eventsDB.save(eventsCache, Date.now());
      if (success) {
        return;
      }
    } catch (e) {
      console.warn('IndexedDB save failed, falling back to localStorage:', e);
    }

    // Fallback to localStorage (may fail for large datasets)
    try {
      localStorage.setItem(CONFIG.EVENTS_CACHE_KEY, JSON.stringify(eventsCache));
      console.log('  Saved events to localStorage (fallback)');
    } catch (e) {
      console.warn('Failed to save events cache to localStorage:', e);
    }
  }

  /**
   * Rebuild the state cache from events
   */
  function rebuildStateCache() {
    stateCache.clear();
    const states = EOMigration.reconstructAllStates(eventsCache);

    for (const state of states) {
      const entityId = state._entity_id || state.Docket_Number;
      if (entityId) {
        stateCache.set(entityId, state);
      }
    }

    console.log(`  Rebuilt state cache: ${stateCache.size} entities`);
  }

  // =============================================================================
  // SYNC
  // =============================================================================

  /**
   * Start background sync
   */
  function startBackgroundSync() {
    if (syncInterval) {
      clearInterval(syncInterval);
    }

    syncInterval = setInterval(async () => {
      try {
        await syncFromEO();
      } catch (e) {
        console.warn('Background sync failed:', e);
      }
    }, CONFIG.SYNC_INTERVAL_MS);

    console.log(`  Background sync started (every ${CONFIG.SYNC_INTERVAL_MS / 1000}s)`);
  }

  /**
   * Stop background sync
   */
  function stopBackgroundSync() {
    if (syncInterval) {
      clearInterval(syncInterval);
      syncInterval = null;
    }
  }

  /**
   * Sync new events from the EO operations table
   *
   * @param {object} options - Sync options
   * @param {boolean} options.usePaginated - Use paginated endpoint (default: false for incremental sync)
   */
  async function syncFromEO(options = {}) {
    console.log('🔄 Syncing from EO...');

    try {
      const response = await EOMigration.fetchEvents({
        since_ts: lastSyncTimestamp,
        source_table: EOMigration.CONFIG.SOURCE_TABLE
      });

      const newEvents = Array.isArray(response)
        ? response
        : (response.items || response.data || []);

      if (newEvents.length === 0) {
        console.log('  No new events');
        return { synced: 0 };
      }

      console.log(`  Fetched ${newEvents.length} new events`);

      // Add to cache
      eventsCache = eventsCache.concat(newEvents);

      // Update last sync timestamp
      const maxTs = Math.max(...newEvents.map(e => e.ts));
      lastSyncTimestamp = maxTs;
      localStorage.setItem(CONFIG.LAST_SYNC_KEY, lastSyncTimestamp.toString());

      // Update state cache incrementally
      for (const event of newEvents) {
        applyEventToStateCache(event);
      }

      // Save to local cache
      await saveEventsToCache();

      console.log(`✅ Synced ${newEvents.length} events`);
      return { synced: newEvents.length };
    } catch (e) {
      console.error('Sync failed:', e);
      throw e;
    }
  }

  // =============================================================================
  // CURRENT STATE SYNC (new model)
  // =============================================================================

  /**
   * Sync from the current-state endpoint (new model).
   * Replaces event replay with direct state reads.
   *
   * @param {object} options - Sync options
   * @param {function} options.onProgress - Progress callback
   * @param {number} options.perPage - Items per page
   * @param {boolean} options.incrementalFromLastSync - If true, only fetches records updated since last sync
   * @returns {object} { synced, entities, pages }
   */
  async function syncFromCurrentState(options = {}) {
    console.log('Syncing from current-state endpoint...');

    try {
      const fetchOptions = {
        onProgress: options.onProgress,
        perPage: options.perPage || EOMigration.CONFIG.DEFAULT_PAGE_SIZE,
        log: console.log
      };

      // Note: API has no server-side filtering; full fetch is always performed.
      // Client-side merging handles incremental updates.
      if (options.incrementalFromLastSync && lastSyncTimestamp > 0) {
        console.log('  Full fetch (will merge client-side since:', new Date(lastSyncTimestamp).toISOString() + ')');
      }

      const result = await EOMigration.fetchAllCurrentState(fetchOptions);

      if (result.states.length === 0) {
        console.log('  No records found');
        return { synced: 0, entities: 0, pages: 0 };
      }

      console.log('  Fetched', result.states.length, 'entities across', result.pagesFetched, 'pages');

      if (options.incrementalFromLastSync && stateCache.size > 0) {
        // Merge new states into existing cache
        for (const state of result.states) {
          const entityId = state._entity_id || state.Docket_Number;
          if (entityId) {
            stateCache.set(entityId, state);
          }
        }
      } else {
        // Full sync - replace cache
        stateCache.clear();
        for (const state of result.states) {
          const entityId = state._entity_id || state.Docket_Number;
          if (entityId) {
            stateCache.set(entityId, state);
          }
        }
      }

      // Update sync timestamp
      const now = Date.now();
      lastSyncTimestamp = now;
      localStorage.setItem(CONFIG.LAST_SYNC_KEY, now.toString());

      // Save to cache
      await saveEventsToCache();

      console.log('Synced', result.states.length, 'entities,', stateCache.size, 'total in cache');
      return {
        synced: result.states.length,
        entities: stateCache.size,
        pages: result.pagesFetched
      };
    } catch (e) {
      console.error('Current-state sync failed:', e);
      throw e;
    }
  }

  /**
   * Create or update a case via the current-state endpoint.
   * Appends the new data as an entry in the `data` array.
   *
   * @param {string} rawDocket - Docket number
   * @param {object} caseData - The case data to store
   * @param {object} options - Additional options
   * @returns {object} API response
   */
  async function pushCaseToCurrentState(rawDocket, caseData, options = {}) {
    const docket = EOMigration.normalizeDocketNumber(rawDocket);
    const body = EOMigration.toCurrentStateFormat(docket, caseData, options);
    const result = await EOMigration.pushCurrentState(body);

    // Update local cache
    const state = { ...caseData, Docket_Number: docket, _entity_id: docket, _last_updated: Date.now() };
    stateCache.set(docket, state);

    return result;
  }

  /**
   * Full sync using the paginated endpoint
   * More efficient for initial sync or full rebuilds
   *
   * @param {object} options - Sync options
   * @param {function} options.onProgress - Progress callback ({ page, pageTotal, itemsFetched, itemsTotal })
   * @param {number} options.perPage - Items per page (default: 3000)
   * @param {boolean} options.incrementalFromLastSync - If true, only fetches events since last sync timestamp
   * @returns {object} { synced, pages, totalItems }
   */
  async function syncFromEOPaginated(options = {}) {
    console.log('🔄 Syncing from EO (paginated)...');

    try {
      const fetchOptions = {
        onProgress: options.onProgress,
        perPage: options.perPage || EOMigration.CONFIG.DEFAULT_PAGE_SIZE,
        log: console.log
      };

      // Optionally do incremental sync from last timestamp
      if (options.incrementalFromLastSync && lastSyncTimestamp > 0) {
        fetchOptions.since_ts = lastSyncTimestamp;
        console.log(`  Incremental sync from: ${new Date(lastSyncTimestamp).toISOString()}`);
      }

      const result = await EOMigration.fetchAllEventsPaginated(fetchOptions);

      if (result.events.length === 0) {
        console.log('  No events found');
        return { synced: 0, pages: 0, totalItems: 0 };
      }

      console.log(`  Fetched ${result.events.length} events across ${result.pagesFetched} pages`);

      // Replace or merge events cache
      if (options.incrementalFromLastSync && eventsCache.length > 0) {
        // Merge new events with existing
        eventsCache = eventsCache.concat(result.events);
      } else {
        // Full sync - replace entire cache
        eventsCache = result.events;
      }

      // Update last sync timestamp
      if (result.events.length > 0) {
        const maxTs = Math.max(...result.events.map(e => e.ts));
        lastSyncTimestamp = maxTs;
        localStorage.setItem(CONFIG.LAST_SYNC_KEY, lastSyncTimestamp.toString());
      }

      // Rebuild state cache from all events
      rebuildStateCache();

      // Save to local cache
      await saveEventsToCache();

      console.log(`✅ Synced ${result.events.length} events, ${stateCache.size} entities`);
      return {
        synced: result.events.length,
        pages: result.pagesFetched,
        totalItems: result.itemsTotal,
        entities: stateCache.size
      };
    } catch (e) {
      console.error('Paginated sync failed:', e);
      throw e;
    }
  }

  /**
   * Check if a value is empty (null, undefined, or empty string)
   * Matches the no-nulls semantics of EOMigration.reconstructEntityState()
   */
  function isEmptyValue(value) {
    return value === null || value === undefined || value === '';
  }

  /**
   * Merge data into state, skipping empty/null/undefined values.
   * This ensures that incremental sync matches the full replay behavior
   * where empty values never overwrite existing data.
   */
  function mergeNonEmpty(target, source) {
    if (!source) return;
    for (const [key, value] of Object.entries(source)) {
      if (!isEmptyValue(value)) {
        target[key] = value;
      }
    }
  }

  /**
   * Apply a single event to the state cache
   *
   * Proper event sourcing implementation:
   * - Handles ALT events even without prior INS event (upsert behavior)
   * - Creates entity from ALT data if it doesn't exist
   * - Tracks changes for all available data
   * - Uses normalized entity IDs for consistent deduplication
   * - Empty/null values do NOT overwrite existing data (matches replay semantics)
   */
  function applyEventToStateCache(event) {
    const rawEntityId = event.entity_id || event.target?.id;
    if (!rawEntityId) return; // Skip events without valid entity ID

    // Normalize the entity ID for consistent cache lookups
    const entityId = EOMigration.normalizeDocketNumber(rawEntityId);
    let state = stateCache.get(entityId);

    // Backwards compatibility: support both 'payload' (new) and 'context' (legacy)
    const payload = event.payload || event.context || {};

    switch (event.op) {
      case EOMigration.OPERATORS.INS:
        // New entity or update existing
        if (state) {
          // Merge into existing state (INS can be re-applied or come out of order)
          // Skip empty values to match replay semantics
          mergeNonEmpty(state, payload.data);
        } else {
          state = {};
          mergeNonEmpty(state, payload.data);
          state.Docket_Number = entityId;
        }
        state._entity_id = entityId;
        state._last_updated = event.ts;
        stateCache.set(entityId, state);
        break;

      case EOMigration.OPERATORS.ALT:
        // Ensure state exists for ALT - create if needed (upsert)
        if (!state) {
          state = {
            Docket_Number: entityId,
            _entity_id: entityId
          };
        }

        // ALT with payload.data = full record upsert
        // Skip empty values to match replay semantics
        mergeNonEmpty(state, payload.data);

        // ALT with payload.changes = delta update
        if (payload.changes) {
          for (const [field, change] of Object.entries(payload.changes)) {
            // change can be { old, new } or just a new value
            const newValue = change.new !== undefined ? change.new : change;
            // Skip empty values - they don't delete fields
            if (!isEmptyValue(newValue)) {
              state[field] = newValue;
            }
          }
        }

        // ALT with single field update via target.field
        if (event.target?.field && payload.new !== undefined) {
          if (!isEmptyValue(payload.new)) {
            state[event.target.field] = payload.new;
          }
        }

        state._last_updated = event.ts;
        stateCache.set(entityId, state);
        break;

    }
  }

  // =============================================================================
  // CRUD OPERATIONS (EO-based)
  // =============================================================================

  /**
   * Create a new case (generates INS event)
   */
  async function createCase(caseData, options = {}) {
    const rawDocket = caseData.Docket_Number || caseData.docket_number;
    if (!rawDocket) {
      throw new Error('Docket number is required');
    }

    // Normalize docket for consistent cache lookup
    const docket = EOMigration.normalizeDocketNumber(rawDocket);

    // Check if case already exists
    if (stateCache.has(docket)) {
      throw new Error(`Case ${docket} already exists`);
    }

    // Create INS event
    const event = EOMigration.createInsertEvent(caseData, {
      source: 'application',
      actor: options.actor || 'user',
      version: '1.0'
    });

    // Push to Xano EO operations table
    if (CONFIG.EO_ENABLED) {
      await EOMigration.pushEvent(event);
    }

    // Update local state
    eventsCache.push(event);
    applyEventToStateCache(event);
    await saveEventsToCache();

    return event;
  }

  /**
   * Update a case (generates ALT event)
   */
  async function updateCase(rawDocket, changes, options = {}) {
    // Normalize docket for consistent cache lookup
    const docket = EOMigration.normalizeDocketNumber(rawDocket);

    // Get current state
    const currentState = stateCache.get(docket);
    if (!currentState) {
      throw new Error(`Case ${docket} not found`);
    }

    // Build change object
    const changeObj = {};
    for (const [field, newValue] of Object.entries(changes)) {
      const oldValue = currentState[field];
      if (JSON.stringify(oldValue) !== JSON.stringify(newValue)) {
        changeObj[field] = { old: oldValue, new: newValue };
      }
    }

    if (Object.keys(changeObj).length === 0) {
      console.log('No changes detected');
      return null;
    }

    // Create ALT event
    const event = EOMigration.createBulkUpdateEvent(docket, changeObj, {
      source: 'application',
      actor: options.actor || 'user',
      version: '1.0'
    });

    // Push to Xano EO operations table
    if (CONFIG.EO_ENABLED) {
      await EOMigration.pushEvent(event);
    }

    // Update local state
    eventsCache.push(event);
    applyEventToStateCache(event);
    await saveEventsToCache();

    return event;
  }

  // =============================================================================
  // QUERY OPERATIONS
  // =============================================================================

  /**
   * Get a case by docket number
   */
  function getCase(rawDocket) {
    const docket = EOMigration.normalizeDocketNumber(rawDocket);
    return stateCache.get(docket) || null;
  }

  /**
   * Get all cases (current state)
   */
  function getAllCases() {
    return Array.from(stateCache.values());
  }

  /**
   * Get cases matching a filter
   */
  function getCases(filterFn) {
    return getAllCases().filter(filterFn);
  }

  /**
   * Get audit trail for a case
   */
  function getCaseAuditTrail(rawDocket) {
    const docket = EOMigration.normalizeDocketNumber(rawDocket);
    return EOMigration.getAuditTrail(docket, eventsCache);
  }

  /**
   * Get operator pattern for a case (for causal analysis)
   */
  function getCasePattern(rawDocket) {
    const docket = EOMigration.normalizeDocketNumber(rawDocket);
    return EOMigration.getOperatorPattern(docket, eventsCache);
  }

  /**
   * Get the complete field-level history for a case
   * Useful for understanding how each field evolved over time.
   */
  function getCaseFieldHistory(rawDocket) {
    const docket = EOMigration.normalizeDocketNumber(rawDocket);
    return EOMigration.getFieldHistory(docket, eventsCache);
  }

  /**
   * Get the latest values for all fields of a case
   * This is the definitive current state based on all available events.
   */
  function getCaseLatestFieldValues(rawDocket) {
    const docket = EOMigration.normalizeDocketNumber(rawDocket);
    return EOMigration.getLatestFieldValues(docket, eventsCache);
  }

  /**
   * Get cases created in a date range
   */
  function getCasesCreatedInRange(startDate, endDate) {
    const startTs = new Date(startDate).getTime();
    const endTs = new Date(endDate).getTime();

    const dockets = EOMigration.getCreatedInRange(eventsCache, startTs, endTs);
    return dockets.map(d => stateCache.get(d)).filter(Boolean);
  }

  /**
   * Get cases with recent changes
   */
  function getRecentlyUpdatedCases(sinceTs) {
    const recentEvents = eventsCache.filter(e =>
      e.ts >= sinceTs && e.op === EOMigration.OPERATORS.ALT
    );

    // Normalize docket numbers for consistent cache lookup
    const dockets = [...new Set(
      recentEvents
        .map(e => EOMigration.normalizeDocketNumber(e.entity_id || e.target?.id))
        .filter(Boolean)
    )];
    return dockets.map(d => stateCache.get(d)).filter(Boolean);
  }

  // =============================================================================
  // STATISTICS
  // =============================================================================

  /**
   * Get EO system statistics
   */
  function getStats() {
    const opCounts = {};
    for (const event of eventsCache) {
      opCounts[event.op] = (opCounts[event.op] || 0) + 1;
    }

    return {
      totalEvents: eventsCache.length,
      activeEntities: stateCache.size,
      operationCounts: opCounts,
      lastSyncTimestamp: lastSyncTimestamp,
      lastSyncDate: new Date(lastSyncTimestamp).toISOString(),
      config: { ...CONFIG }
    };
  }

  // =============================================================================
  // MIGRATION HELPERS
  // =============================================================================

  /**
   * Bootstrap EO from existing application data
   */
  async function bootstrapFromLegacy(legacyData) {
    console.log('🔧 Bootstrapping EO from legacy data...');

    // Convert to events
    const events = EOMigration.convertSnapshotToEvents(legacyData);
    console.log(`  Generated ${events.length} INS events`);

    // Push to Xano
    const results = await EOMigration.pushEventsBatch(events, (progress) => {
      console.log(`  Progress: ${progress.processed}/${progress.total}`);
    });

    console.log(`  Push complete: ${results.success} success, ${results.failed} failed`);

    // Update local cache
    eventsCache = eventsCache.concat(events);
    rebuildStateCache();
    await saveEventsToCache();

    return results;
  }

  /**
   * Verify EO state matches legacy data
   */
  function verifyStateConsistency(legacyData) {
    const issues = [];

    for (const legacyRecord of legacyData) {
      const docket = legacyRecord.Docket_Number || legacyRecord.docket_number;
      if (!docket) continue;

      const eoState = stateCache.get(docket);

      if (!eoState) {
        issues.push({ docket, issue: 'missing_in_eo' });
        continue;
      }

      // Check key fields
      const fieldsToCheck = ['Status', 'Plaintiff_Petitioner', 'Defendant_Respondent', 'File_Date'];
      for (const field of fieldsToCheck) {
        const legacyVal = legacyRecord[field];
        const eoVal = eoState[field];

        if (legacyVal && eoVal && legacyVal !== eoVal) {
          issues.push({
            docket,
            issue: 'field_mismatch',
            field,
            legacy: legacyVal,
            eo: eoVal
          });
        }
      }
    }

    return {
      totalChecked: legacyData.length,
      issueCount: issues.length,
      issues: issues.slice(0, 100) // Limit for display
    };
  }

  // =============================================================================
  // BINARY SNAPSHOT - Export & Hydration
  // =============================================================================

  /**
   * Binary snapshot format (.bin):
   *
   * Header (17 bytes):
   *   [0-3]   Magic: "EVIC" (4 bytes)
   *   [4]     Version: 1 (1 byte)
   *   [5-12]  Snapshot timestamp as float64 (8 bytes) - ms since epoch
   *   [13-16] Entity count as uint32 (4 bytes)
   *
   * Field Dictionary:
   *   [17-18] Number of fields as uint16 (2 bytes)
   *   For each field:
   *     [1 byte] field name length
   *     [N bytes] field name (UTF-8)
   *
   * Entity Records:
   *   For each entity:
   *     [2 bytes] number of field-value pairs as uint16
   *     For each pair:
   *       [2 bytes] field index (into dictionary) as uint16
   *       [1 byte]  value type tag:
   *                   0 = null/empty
   *                   1 = string (uint16 length + UTF-8 bytes)
   *                   2 = number (float64, 8 bytes)
   *                   3 = boolean (1 byte: 0 or 1)
   *       [N bytes] value data (per type tag)
   */

  const SNAPSHOT_MAGIC = 'EVIC';
  const SNAPSHOT_VERSION = 1;

  /**
   * Export current stateCache as a binary snapshot file (.bin)
   * @returns {Blob} Binary blob ready for download
   */
  function exportBinarySnapshot() {
    const entities = Array.from(stateCache.values());
    const snapshotTs = lastSyncTimestamp || Date.now();

    // Step 1: Build field dictionary from all entities
    const fieldSet = new Set();
    for (const entity of entities) {
      for (const key of Object.keys(entity)) {
        fieldSet.add(key);
      }
    }
    const fieldList = Array.from(fieldSet);
    const fieldIndex = new Map();
    fieldList.forEach((f, i) => fieldIndex.set(f, i));

    // Step 2: Estimate buffer size (generous) and encode
    const encoder = new TextEncoder();

    // Pre-encode all strings to measure size
    const encodedFieldNames = fieldList.map(f => encoder.encode(f));
    let estimatedSize = 17; // header
    estimatedSize += 2; // field count
    for (const ef of encodedFieldNames) {
      estimatedSize += 1 + ef.byteLength; // length byte + name
    }

    // Estimate entity data size
    for (const entity of entities) {
      estimatedSize += 2; // pair count
      for (const [key, value] of Object.entries(entity)) {
        estimatedSize += 2 + 1; // field index + type tag
        if (value === null || value === undefined || value === '') {
          // type 0: no extra bytes
        } else if (typeof value === 'number') {
          estimatedSize += 8;
        } else if (typeof value === 'boolean') {
          estimatedSize += 1;
        } else {
          const strBytes = encoder.encode(String(value));
          estimatedSize += 4 + strBytes.byteLength; // uint32 length + bytes
        }
      }
    }

    // Allocate buffer with some headroom
    const buffer = new ArrayBuffer(estimatedSize + 1024);
    const view = new DataView(buffer);
    let offset = 0;

    // Header: magic bytes
    for (let i = 0; i < 4; i++) {
      view.setUint8(offset++, SNAPSHOT_MAGIC.charCodeAt(i));
    }
    // Version
    view.setUint8(offset++, SNAPSHOT_VERSION);
    // Timestamp (float64)
    view.setFloat64(offset, snapshotTs);
    offset += 8;
    // Entity count (uint32)
    view.setUint32(offset, entities.length);
    offset += 4;

    // Field dictionary
    view.setUint16(offset, fieldList.length);
    offset += 2;
    for (const encoded of encodedFieldNames) {
      view.setUint8(offset++, encoded.byteLength);
      new Uint8Array(buffer, offset, encoded.byteLength).set(encoded);
      offset += encoded.byteLength;
    }

    // Entity records
    for (const entity of entities) {
      const entries = Object.entries(entity).filter(([, v]) => v !== undefined);
      view.setUint16(offset, entries.length);
      offset += 2;

      for (const [key, value] of entries) {
        // Field index
        view.setUint16(offset, fieldIndex.get(key));
        offset += 2;

        if (value === null || value === '') {
          // Type 0: null/empty
          view.setUint8(offset++, 0);
        } else if (typeof value === 'number') {
          // Type 2: number
          view.setUint8(offset++, 2);
          view.setFloat64(offset, value);
          offset += 8;
        } else if (typeof value === 'boolean') {
          // Type 3: boolean
          view.setUint8(offset++, 3);
          view.setUint8(offset++, value ? 1 : 0);
        } else {
          // Type 1: string
          view.setUint8(offset++, 1);
          const strBytes = encoder.encode(String(value));
          view.setUint32(offset, strBytes.byteLength);
          offset += 4;
          new Uint8Array(buffer, offset, strBytes.byteLength).set(strBytes);
          offset += strBytes.byteLength;
        }
      }
    }

    // Trim buffer to actual size
    const trimmed = buffer.slice(0, offset);

    console.log(`Binary snapshot: ${entities.length} entities, ${fieldList.length} fields, ${offset} bytes`);

    return new Blob([trimmed], { type: 'application/octet-stream' });
  }

  /**
   * Download the binary snapshot as a file
   */
  function downloadBinarySnapshot() {
    const blob = exportBinarySnapshot();
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
    const filename = `evictions-snapshot-${timestamp}.bin`;

    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);

    console.log(`Downloaded snapshot: ${filename} (${(blob.size / 1024).toFixed(1)} KB)`);
    return { filename, size: blob.size };
  }

  /**
   * Parse a binary snapshot file back into state data
   * @param {ArrayBuffer} buffer - The .bin file contents
   * @returns {{ timestamp: number, entities: Array<object> }} Parsed snapshot data
   */
  function parseBinarySnapshot(buffer) {
    const view = new DataView(buffer);
    const decoder = new TextDecoder();
    let offset = 0;

    // Validate magic
    const magic = String.fromCharCode(
      view.getUint8(0), view.getUint8(1), view.getUint8(2), view.getUint8(3)
    );
    if (magic !== SNAPSHOT_MAGIC) {
      throw new Error(`Invalid snapshot file: expected magic "${SNAPSHOT_MAGIC}", got "${magic}"`);
    }
    offset = 4;

    // Version
    const version = view.getUint8(offset++);
    if (version !== SNAPSHOT_VERSION) {
      throw new Error(`Unsupported snapshot version: ${version}`);
    }

    // Timestamp
    const snapshotTs = view.getFloat64(offset);
    offset += 8;

    // Entity count
    const entityCount = view.getUint32(offset);
    offset += 4;

    // Field dictionary
    const fieldCount = view.getUint16(offset);
    offset += 2;
    const fieldList = [];
    for (let i = 0; i < fieldCount; i++) {
      const nameLen = view.getUint8(offset++);
      const nameBytes = new Uint8Array(buffer, offset, nameLen);
      fieldList.push(decoder.decode(nameBytes));
      offset += nameLen;
    }

    // Entity records
    const entities = [];
    for (let e = 0; e < entityCount; e++) {
      const pairCount = view.getUint16(offset);
      offset += 2;
      const entity = {};

      for (let p = 0; p < pairCount; p++) {
        const fIdx = view.getUint16(offset);
        offset += 2;
        const fieldName = fieldList[fIdx];

        const typeTag = view.getUint8(offset++);
        switch (typeTag) {
          case 0: // null/empty
            entity[fieldName] = null;
            break;
          case 1: // string
            const strLen = view.getUint32(offset);
            offset += 4;
            const strBytes = new Uint8Array(buffer, offset, strLen);
            entity[fieldName] = decoder.decode(strBytes);
            offset += strLen;
            break;
          case 2: // number
            entity[fieldName] = view.getFloat64(offset);
            offset += 8;
            break;
          case 3: // boolean
            entity[fieldName] = view.getUint8(offset++) === 1;
            break;
          default:
            throw new Error(`Unknown type tag ${typeTag} at offset ${offset - 1}`);
        }
      }
      entities.push(entity);
    }

    console.log(`Parsed snapshot: ${entities.length} entities, ts=${new Date(snapshotTs).toISOString()}`);
    return { timestamp: snapshotTs, entities };
  }

  /**
   * Hydrate the app state from a binary snapshot file.
   * Loads the snapshot into stateCache and sets lastSyncTimestamp,
   * enabling incremental replay for events that arrived after the snapshot.
   *
   * @param {ArrayBuffer} buffer - The .bin file contents
   * @returns {{ entityCount: number, timestamp: number }}
   */
  function hydrateFromBinarySnapshot(buffer) {
    const { timestamp, entities } = parseBinarySnapshot(buffer);

    // Clear and rebuild stateCache from snapshot
    stateCache.clear();
    for (const entity of entities) {
      const entityId = entity._entity_id || entity.Docket_Number;
      if (entityId) {
        stateCache.set(entityId, entity);
      }
    }

    // Set lastSyncTimestamp so incremental sync fetches only newer events
    lastSyncTimestamp = timestamp;
    localStorage.setItem(CONFIG.LAST_SYNC_KEY, timestamp.toString());

    // Clear the events cache since we're starting from a snapshot, not from events
    eventsCache = [];

    console.log(`Hydrated from snapshot: ${stateCache.size} entities, sync from ${new Date(timestamp).toISOString()}`);

    return { entityCount: stateCache.size, timestamp };
  }

  /**
   * Get snapshot metadata (for display in UI)
   */
  function getSnapshotInfo() {
    return {
      entityCount: stateCache.size,
      lastSyncTimestamp,
      lastSyncDate: lastSyncTimestamp ? new Date(lastSyncTimestamp).toISOString() : null,
      eventsInCache: eventsCache.length
    };
  }

  // =============================================================================
  // CONFIGURATION
  // =============================================================================

  /**
   * Update configuration
   */
  function configure(updates) {
    Object.assign(CONFIG, updates);

    // Restart sync if interval changed
    if (updates.SYNC_INTERVAL_MS && CONFIG.EO_ENABLED) {
      stopBackgroundSync();
      startBackgroundSync();
    }
  }

  /**
   * Enable/disable EO
   */
  function setEnabled(enabled) {
    CONFIG.EO_ENABLED = enabled;
    if (enabled) {
      startBackgroundSync();
    } else {
      stopBackgroundSync();
    }
  }

  // =============================================================================
  // PUBLIC API
  // =============================================================================

  return {
    // Initialization
    initialize,

    // Sync (legacy event-sourcing)
    syncFromEO,
    syncFromEOPaginated,
    startBackgroundSync,
    stopBackgroundSync,

    // Sync (current-state model)
    syncFromCurrentState,
    pushCaseToCurrentState,

    // CRUD operations
    createCase,
    updateCase,

    // Query operations
    getCase,
    getAllCases,
    getCases,
    getCaseAuditTrail,
    getCasePattern,
    getCaseFieldHistory,
    getCaseLatestFieldValues,
    getCasesCreatedInRange,
    getRecentlyUpdatedCases,

    // Statistics
    getStats,

    // Migration
    bootstrapFromLegacy,
    verifyStateConsistency,

    // Binary Snapshot
    exportBinarySnapshot,
    downloadBinarySnapshot,
    parseBinarySnapshot,
    hydrateFromBinarySnapshot,
    getSnapshotInfo,

    // Configuration
    configure,
    setEnabled,
    CONFIG
  };
})();

// Export for Node.js environments
if (typeof module !== 'undefined' && module.exports) {
  module.exports = EOIntegration;
}

// Usage examples (commented out for reference):
/*

// Initialize the integration
await EOIntegration.initialize();

// Get stats
console.log(EOIntegration.getStats());

// Create a new case
const event = await EOIntegration.createCase({
  Docket_Number: '24GC9999',
  File_Date: '01/15/2024',
  Status: 'Active',
  Plaintiff_Petitioner: 'ABC Properties LLC',
  Defendant_Respondent: 'John Doe'
});

// Update a case
const updateEvent = await EOIntegration.updateCase('24GC9999', {
  Status: 'Disposed',
  judgment_for: 'Plaintiff'
});

// Get case with audit trail
const caseData = EOIntegration.getCase('24GC9999');
const auditTrail = EOIntegration.getCaseAuditTrail('24GC9999');
console.log('Case:', caseData);
console.log('Audit trail:', auditTrail);

// Get operator pattern (for understanding case lifecycle)
const pattern = EOIntegration.getCasePattern('24GC9999');
console.log('Pattern:', pattern.pattern); // e.g., "INS→ALT→ALT"

// Bootstrap EO from existing data in the app
const allData = window.allData; // Existing application data
await EOIntegration.bootstrapFromLegacy(allData);

*/
