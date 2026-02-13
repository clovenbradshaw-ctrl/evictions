/**
 * EO (Epistemic Operators) Migration Script
 *
 * Migrates Nashville eviction tracker data to an append-only event-sourced database
 * using the Epistemic Operators grammar.
 *
 * Operations Table Schema (Xano: evictionsEOevents):
 * - id: UUID (primary key)
 * - ts: INTEGER (Unix timestamp in milliseconds - when event was synced/recorded)
 * - ts_iso: TIMESTAMP (ISO formatted timestamp)
 * - op: TEXT (INS, ALT)
 * - entity_id: TEXT (case docket number)
 * - entity_type: TEXT (e.g., 'eviction_case')
 * - source_table: TEXT (e.g., 'eviction_cases')
 * - target: TEXT (JSON stringified - what's being operated on)
 * - payload: TEXT (JSON stringified - the data/changes and observation metadata)
 *   - payload.observationTS: INTEGER (Unix timestamp when data was observed/scraped)
 *   - payload.table: TEXT (source table name)
 *   - payload.data: OBJECT (full record for INS, partial for ALT)
 *   - payload.changes: OBJECT (field changes for ALT operations)
 * - frame: TEXT (JSON stringified - optional interpretive context)
 */

const EOMigration = (function() {
  'use strict';

  // =============================================================================
  // CONFIGURATION
  // =============================================================================

  const CONFIG = {
    // EO operations endpoints (legacy event-sourcing API)
    OPERATIONS_GET_URL: 'https://xvkq-pq7i-idtl.n7d.xano.io/api:3CsVHkZK/eviction_operations',
    // Paginated endpoint for efficient full syncs
    OPERATIONS_PAGINATED_URL: 'https://xvkq-pq7i-idtl.n7d.xano.io/api:3CsVHkZK/evictionseoeventsPage',
    // POST URL is encrypted and must be set via setPostEndpoint() after authentication
    OPERATIONS_POST_URL: null,

    // Current-state API endpoints (new model)
    CURRENT_STATE_GET_URL: 'https://xvkq-pq7i-idtl.n7d.xano.io/api:3CsVHkZK/eviction_current_state',
    CURRENT_STATE_POST_URL: 'https://xvkq-pq7i-idtl.n7d.xano.io/api:3CsVHkZK/eviction_current_state',

    // Append-only update endpoint (upserts by docket_number, appends to stateData array)
    UPDATE_STATE_URL: 'https://xvkq-pq7i-idtl.n7d.xano.io/api:3CsVHkZK/update_eviction_state',

    // Source table identifier
    SOURCE_TABLE: 'eviction_cases',

    // Batch size for migrations
    BATCH_SIZE: 100,

    // Default page size for paginated fetches (API default is 2500)
    DEFAULT_PAGE_SIZE: 2500
  };

  /**
   * Set the POST endpoint URL (called after decryption on login)
   */
  function setPostEndpoint(url) {
    CONFIG.OPERATIONS_POST_URL = url;
  }

  /**
   * Check if POST endpoint is available
   */
  function isPostEndpointAvailable() {
    return CONFIG.OPERATIONS_POST_URL !== null;
  }

  // =============================================================================
  // EO OPERATORS
  // =============================================================================

  /**
   * Epistemic Operators
   */
  const OPERATORS = {
    INS: 'INS',  // Creating/instantiating
    ALT: 'ALT'   // Alternating/transitioning state
  };

  /**
   * Operator semantics for eviction filing tracking
   */
  const OPERATOR_MEANINGS = {
    INS: 'New eviction filing recorded',
    ALT: 'Filing data updated'
  };

  // =============================================================================
  // UUID GENERATION
  // =============================================================================

  /**
   * Generate a UUID v4
   */
  function generateUUID() {
    if (typeof crypto !== 'undefined' && crypto.randomUUID) {
      return crypto.randomUUID();
    }
    // Fallback for older browsers
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
      const r = Math.random() * 16 | 0;
      const v = c === 'x' ? r : (r & 0x3 | 0x8);
      return v.toString(16);
    });
  }

  // =============================================================================
  // DOCKET NUMBER NORMALIZATION
  // =============================================================================

  /**
   * Normalize a docket number for consistent deduplication
   * Handles whitespace, case sensitivity, and common formatting variations
   *
   * @param {string} docketNumber - The raw docket number
   * @returns {string} - Normalized docket number
   */
  function normalizeDocketNumber(docketNumber) {
    if (!docketNumber) return '';

    // Convert to string if needed
    let normalized = String(docketNumber);

    // Trim whitespace
    normalized = normalized.trim();

    // Remove any internal multiple spaces
    normalized = normalized.replace(/\s+/g, ' ');

    // Docket numbers are typically uppercase, normalize case
    // Note: Nashville docket format is typically like "24GC1-12345"
    normalized = normalized.toUpperCase();

    return normalized;
  }

  // =============================================================================
  // EVENT CREATION
  // =============================================================================

  /**
   * Create a base EO event structure (internal format)
   *
   * @param {string} op - The operation type (INS, ALT)
   * @param {object} target - What's being operated on { id, type, field? }
   * @param {object} payload - The data/changes including:
   *   - table: source table name
   *   - data: full record (for INS) or changed data
   *   - changes: field changes (for ALT operations)
   *   - observationTS: when the data was observed (optional, defaults to now)
   * @param {object} frame - Optional interpretive context { source, version, ... }
   */
  function createEvent(op, target, payload, frame = null) {
    const ts = Date.now();

    // Ensure observationTS is set - defaults to current time if not provided
    const payloadWithObservation = {
      ...payload,
      observationTS: payload.observationTS || ts
    };

    const event = {
      id: generateUUID(),
      ts: ts,
      ts_iso: new Date(ts).toISOString(),
      op: op,
      entity_id: target.id || '',
      entity_type: target.type || 'eviction_case',
      source_table: payloadWithObservation.table || CONFIG.SOURCE_TABLE,
      target: target,
      payload: payloadWithObservation,
      frame: frame || {}
    };

    return event;
  }

  /**
   * Convert an event to Xano table format (TEXT columns for JSON fields)
   * Note: 'payload' replaces the legacy 'context' field
   */
  function toXanoFormat(event) {
    // Support both legacy 'context' and new 'payload' for backwards compatibility during transition
    const payloadData = event.payload || event.context || {};

    return {
      id: event.id,
      ts: event.ts,
      ts_iso: event.ts_iso,
      op: event.op,
      entity_id: event.entity_id,
      entity_type: event.entity_type,
      source_table: event.source_table,
      target: JSON.stringify(event.target),
      payload: JSON.stringify(payloadData),
      frame: JSON.stringify(event.frame || {})
    };
  }

  /**
   * Safely parse JSON, returning fallback on error
   */
  function safeJsonParse(value, fallback = null) {
    if (typeof value !== 'string') {
      return value ?? fallback;
    }
    try {
      return JSON.parse(value);
    } catch (e) {
      console.warn('Failed to parse JSON in event record:', e.message, value);
      return fallback;
    }
  }

  /**
   * Convert from Xano format back to internal format (parse JSON TEXT fields)
   * Supports both legacy 'context' and new 'payload' fields for backwards compatibility
   */
  function fromXanoFormat(record) {
    // Support both legacy 'context' and new 'payload' field names
    const payloadData = safeJsonParse(record.payload || record.context, {});

    return {
      id: record.id,
      ts: record.ts,
      ts_iso: record.ts_iso,
      op: record.op,
      entity_id: record.entity_id,
      entity_type: record.entity_type,
      source_table: record.source_table,
      target: safeJsonParse(record.target, {}),
      payload: payloadData,
      frame: safeJsonParse(record.frame, {})
    };
  }

  /**
   * Get the observation timestamp for an event
   * This is the time the data was actually observed/scraped, used for ordering during replay.
   * Falls back to ts (sync time) if observationTS is not set.
   */
  function getObservationTS(event) {
    return event.payload?.observationTS || event.context?.observationTS || event.ts;
  }

  /**
   * Create an INS (insert/create) event for a new eviction filing
   *
   * @param {object} caseData - The case record data
   * @param {object} frame - Optional interpretive context { source, version, ... }
   * @param {number} observationTS - Optional timestamp when data was observed (e.g., PDF creation date)
   */
  function createInsertEvent(caseData, frame = null, observationTS = null) {
    const rawDocketNumber = caseData.Docket_Number || caseData.docket_number;

    if (!rawDocketNumber) {
      throw new Error('Cannot create INS event: missing docket number');
    }

    // Normalize docket number for consistent deduplication during replay
    const docketNumber = normalizeDocketNumber(rawDocketNumber);

    // Normalize the docket in the case data too for consistency
    const normalizedCaseData = { ...caseData };
    if (normalizedCaseData.Docket_Number) {
      normalizedCaseData.Docket_Number = docketNumber;
    }
    if (normalizedCaseData.docket_number) {
      normalizedCaseData.docket_number = docketNumber;
    }

    const payload = {
      table: CONFIG.SOURCE_TABLE,
      data: normalizedCaseData
    };

    // Add observationTS if provided
    if (observationTS) {
      payload.observationTS = observationTS;
    }

    return createEvent(
      OPERATORS.INS,
      {
        id: docketNumber,
        type: 'eviction_case'
      },
      payload,
      frame || {
        source: 'migration',
        version: '1.0'
      }
    );
  }

  /**
   * Create an ALT (update/transition) event for case field changes
   */
  function createUpdateEvent(docketNumber, fieldName, oldValue, newValue, frame = null) {
    // Normalize docket number for consistent deduplication during replay
    const normalizedDocket = normalizeDocketNumber(docketNumber);

    return createEvent(
      OPERATORS.ALT,
      {
        id: normalizedDocket,
        field: fieldName
      },
      {
        table: CONFIG.SOURCE_TABLE,
        old: oldValue,
        new: newValue
      },
      frame || {
        source: 'update',
        version: '1.0'
      }
    );
  }

  /**
   * Create a bulk ALT event for multiple field changes
   *
   * @param {string} docketNumber - The case docket number
   * @param {object} changes - Field changes { fieldName: { old, new }, ... }
   * @param {object} frame - Optional interpretive context { source, version, ... }
   * @param {number} observationTS - Optional timestamp when data was observed (e.g., PDF creation date)
   */
  function createBulkUpdateEvent(docketNumber, changes, frame = null, observationTS = null) {
    // Normalize docket number for consistent deduplication during replay
    const normalizedDocket = normalizeDocketNumber(docketNumber);

    // changes = { fieldName: { old: oldValue, new: newValue }, ... }
    const payload = {
      table: CONFIG.SOURCE_TABLE,
      changes: changes
    };

    // Add observationTS if provided
    if (observationTS) {
      payload.observationTS = observationTS;
    }

    return createEvent(
      OPERATORS.ALT,
      {
        id: normalizedDocket,
        type: 'eviction_case'
      },
      payload,
      frame || {
        source: 'bulk_update',
        version: '1.0'
      }
    );
  }

  // =============================================================================
  // DATA NORMALIZATION
  // =============================================================================

  /**
   * Normalize a case record to consistent field names
   */
  function normalizeCase(record) {
    // Handle case_data wrapper if present
    let data = record;
    if (record.case_data) {
      if (typeof record.case_data === 'string') {
        try {
          data = { ...record, ...JSON.parse(record.case_data) };
        } catch (e) {
          console.warn('Failed to parse case_data:', e);
        }
      } else {
        data = { ...record, ...record.case_data };
      }
    }

    // Normalize to canonical field names (PascalCase for entity fields)
    return {
      Docket_Number: data.Docket_Number || data.docket_number,
      File_Date: data.File_Date || data.file_date,
      Status: data.Status || data.status,
      Office: data.Office || data.office,
      Description: data.Description || data.description,
      Plaintiff_Petitioner: data.Plaintiff_Petitioner || data.plaintiff_petitioner || data.plaintiff,
      Defendant_Respondent: data.Defendant_Respondent || data.defendant_respondent || data.defendant,
      Attorney: data.Attorney || data.attorney || data.plaintiff_attorney,
      Defendant_Attorney: data.Defendant_Attorney || data.defendant_attorney || data.defense_attorney,
      Address_1: data.Address_1 || data.address_1 || data.address,
      Address_2: data.Address_2 || data.address_2,
      city_state_zip: data.city_state_zip || data.City_State_Zip,
      address_formatted: data.address_formatted || data.formatted_address,
      latitude: data.latitude || data.lat,
      longitude: data.longitude || data.lng || data.lon,
      council_district: data.council_district || data.Council_District,
      council_district_number: data.council_district_number || data.Council_District_Number,
      council_member: data.council_member || data.Council_Member,
      individual_ll: data.individual_ll || data.Individual_LL,
      // Judgment/disposition fields
      judge: data.judge,
      pleadings: data.pleadings || [],
      judgments: data.judgments || [],
      filing_date: data.filing_date,
      service_date: data.service_date,
      judgment_for: data.judgment_for,
      judgment_date: data.judgment_date,
      judgment_type: data.judgment_type,
      case_dismissed: data.case_dismissed,
      dismissal_date: data.dismissal_date,
      eviction_date: data.eviction_date,
      eviction_method: data.eviction_method,
      eviction_executed: data.eviction_executed,
      writ_issued_date: data.writ_issued_date,
      writ_served_date: data.writ_served_date,
      paper_writ_date: data.paper_writ_date
    };
  }

  /**
   * Compare two case records and return changed fields
   */
  function diffCases(oldCase, newCase) {
    const normalizedOld = normalizeCase(oldCase);
    const normalizedNew = normalizeCase(newCase);
    const changes = {};

    for (const key of Object.keys(normalizedNew)) {
      const oldVal = normalizedOld[key];
      const newVal = normalizedNew[key];

      // Skip if both are null/undefined
      if (oldVal == null && newVal == null) continue;

      // Compare values (handle arrays/objects)
      const oldStr = JSON.stringify(oldVal);
      const newStr = JSON.stringify(newVal);

      if (oldStr !== newStr) {
        changes[key] = {
          old: oldVal,
          new: newVal
        };
      }
    }

    return Object.keys(changes).length > 0 ? changes : null;
  }

  // =============================================================================
  // MIGRATION FROM LEGACY FORMAT
  // =============================================================================

  /**
   * Convert legacy activity stream records to EO events
   *
   * The current Xano data already has activity-stream-like structure:
   * - logged_at: timestamp
   * - action: type of change
   * - case_data: snapshot of case at that time
   * - docket_number: case identifier
   */
  function convertLegacyActivityToEvents(activityRecords) {
    const events = [];
    const caseFirstSeen = new Map(); // Track first occurrence of each case

    // Sort by logged_at to ensure chronological processing
    const sorted = [...activityRecords].sort((a, b) => {
      const aTime = new Date(a.logged_at || 0).getTime();
      const bTime = new Date(b.logged_at || 0).getTime();
      return aTime - bTime;
    });

    for (const record of sorted) {
      const docket = record.docket_number;
      if (!docket) continue;

      const ts = new Date(record.logged_at || Date.now()).getTime();
      const action = record.action || 'unknown';

      // First time seeing this case = INS event
      if (!caseFirstSeen.has(docket)) {
        caseFirstSeen.set(docket, record);

        events.push({
          id: generateUUID(),
          ts: ts,
          op: OPERATORS.INS,
          target: {
            id: docket,
            type: 'eviction_case'
          },
          payload: {
            table: CONFIG.SOURCE_TABLE,
            data: record,
            observationTS: ts // Legacy migration uses logged_at as observation time
          },
          frame: {
            source: 'migration',
            legacy_id: record.id,
            legacy_action: action,
            version: '1.0'
          }
        });
      } else {
        // Subsequent occurrences = ALT events (if data changed)
        const previousRecord = caseFirstSeen.get(docket);
        const changes = diffCases(previousRecord, record);

        if (changes) {
          events.push({
            id: generateUUID(),
            ts: ts,
            op: OPERATORS.ALT,
            target: {
              id: docket,
              type: 'eviction_case'
            },
            payload: {
              table: CONFIG.SOURCE_TABLE,
              changes: changes,
              observationTS: ts // Legacy migration uses logged_at as observation time
            },
            frame: {
              source: 'migration',
              legacy_id: record.id,
              legacy_action: action,
              version: '1.0'
            }
          });

          // Update the reference for future diffs
          caseFirstSeen.set(docket, record);
        }
      }
    }

    return events;
  }

  /**
   * Convert flat case records (current state) to INS events
   * Use this when you only have the current snapshot, not activity history
   */
  function convertSnapshotToEvents(caseRecords, migrationTimestamp = Date.now()) {
    const events = [];

    for (const record of caseRecords) {
      const docket = record.Docket_Number || record.docket_number;
      if (!docket) continue;

      events.push({
        id: generateUUID(),
        ts: migrationTimestamp,
        op: OPERATORS.INS,
        target: {
          id: docket,
          type: 'eviction_case'
        },
        payload: {
          table: CONFIG.SOURCE_TABLE,
          data: record,
          observationTS: migrationTimestamp // Snapshot uses migration timestamp as observation time
        },
        frame: {
          source: 'snapshot_migration',
          original_id: record.id,
          version: '1.0',
          epistemic: 'GIVEN' // External/observed data
        }
      });
    }

    return events;
  }

  // =============================================================================
  // STATE RECONSTRUCTION
  // =============================================================================

  /**
   * Reconstruct current state of a single entity from events
   *
   * Simple cumulative replay:
   * - Sort events by ts (sync time) for stable ordering
   * - First event (INS or ALT) creates the record
   * - Subsequent events accumulate fields
   * - Null/empty values NEVER overwrite existing data
   * - Supports both legacy 'context' and new 'payload' field names
   */
  function reconstructEntityState(entityId, events) {
    // Normalize the entityId for comparison to handle legacy data variations
    const normalizedEntityId = normalizeDocketNumber(entityId);

    const entityEvents = events
      .filter(e => {
        const eventEntityId = e.entity_id || e.target?.id;
        return normalizeDocketNumber(eventEntityId) === normalizedEntityId;
      })
      // Sort by ts (sync time) for stable, simple ordering
      .sort((a, b) => a.ts - b.ts);

    if (entityEvents.length === 0) {
      return null;
    }

    // Simple cumulative state - no field versioning needed
    let state = {};

    // Fields that represent original case properties and should not be overwritten
    // by subsequent payload.data merges (e.g., from rescraping old cases on newer dockets).
    // These use "first-write-wins" semantics for payload.data, but can still be
    // explicitly updated via payload.changes or target.field (intentional corrections).
    const PROTECTED_FIELDS = new Set(['File_Date', 'file_date', 'filing_date']);

    /**
     * Check if a value is empty (should not overwrite existing data)
     */
    function isEmpty(value) {
      if (value === null || value === undefined || value === '') return true;
      if (Array.isArray(value) && value.length === 0) return true;
      return false;
    }

    /**
     * Merge non-empty fields from source into state.
     * Non-empty values always overwrite; empty values never overwrite.
     * Protected fields (like File_Date) use first-write-wins: once set,
     * they are not overwritten by payload.data merges to prevent rescrape
     * events from clobbering original filing dates with later court dates.
     */
    function mergeFields(data) {
      if (!data) return;
      for (const [field, value] of Object.entries(data)) {
        if (!isEmpty(value)) {
          if (PROTECTED_FIELDS.has(field) && !isEmpty(state[field])) {
            continue; // Preserve original value
          }
          state[field] = value;
        }
      }
    }

    for (const event of entityEvents) {
      const payload = event.payload || event.context || {};

      // INS or ALT - same logic: merge non-empty fields cumulatively
      if (payload.data) {
        mergeFields(payload.data);
      }

      // ALT with payload.changes = delta update
      if (payload.changes) {
        for (const [field, change] of Object.entries(payload.changes)) {
          const newValue = change.new !== undefined ? change.new : change;
          if (!isEmpty(newValue)) {
            state[field] = newValue;
          }
        }
      }

      // ALT with single field update via target.field
      if (event.target && event.target.field && payload.new !== undefined) {
        if (!isEmpty(payload.new)) {
          state[event.target.field] = payload.new;
        }
      }
    }

    if (Object.keys(state).length === 0) {
      return null;
    }

    // Ensure Docket_Number is set with normalized value
    state.Docket_Number = normalizedEntityId;
    state._entity_id = normalizedEntityId;
    state._last_updated = entityEvents[entityEvents.length - 1].ts;
    state._event_count = entityEvents.length;

    return state;
  }

  /**
   * Reconstruct current state of all entities
   *
   * IMPORTANT: Normalizes entity IDs (docket numbers) during grouping to ensure
   * events with slightly different formatting (whitespace, case) are properly
   * grouped together. This handles legacy events that may not have been normalized.
   */
  function reconstructAllStates(events) {
    // Group events by NORMALIZED entity ID to handle legacy data variations
    const byEntity = new Map();

    for (const event of events) {
      // Handle events with missing or null target - use entity_id as fallback
      const rawEntityId = event.entity_id || event.target?.id;
      if (!rawEntityId) {
        // Skip events without a valid entity ID
        continue;
      }

      // Normalize the entity ID to group events consistently
      // This ensures "24GC1-12345" and " 24GC1-12345 " and "24gc1-12345" all group together
      const normalizedEntityId = normalizeDocketNumber(rawEntityId);

      if (!byEntity.has(normalizedEntityId)) {
        byEntity.set(normalizedEntityId, []);
      }
      byEntity.get(normalizedEntityId).push(event);
    }

    // Reconstruct each entity using the normalized ID
    const states = [];
    for (const [entityId, entityEvents] of byEntity) {
      const state = reconstructEntityState(entityId, entityEvents);
      if (state) {
        states.push(state);
      }
    }

    return states;
  }

  /**
   * Get the audit trail for an entity
   */
  function getAuditTrail(entityId, events) {
    const normalizedEntityId = normalizeDocketNumber(entityId);
    return events
      .filter(e => normalizeDocketNumber(e.entity_id || e.target?.id) === normalizedEntityId)
      .sort((a, b) => a.ts - b.ts)
      .map(e => ({
        id: e.id,
        when: new Date(e.ts).toISOString(),
        op: e.op,
        meaning: OPERATOR_MEANINGS[e.op] || e.op,
        details: e.payload || e.context,
        frame: e.frame
      }));
  }

  /**
   * Get the operator pattern for an entity (for causal analysis)
   */
  function getOperatorPattern(entityId, events) {
    const normalizedEntityId = normalizeDocketNumber(entityId);
    const ops = events
      .filter(e => normalizeDocketNumber(e.entity_id || e.target?.id) === normalizedEntityId)
      .sort((a, b) => a.ts - b.ts)
      .map(e => e.op);

    return {
      pattern: ops.join('→'),
      ops: ops,
      count: ops.length
    };
  }

  /**
   * Get the complete field-level history for an entity
   *
   * Returns a map of field names to their version history:
   * {
   *   fieldName: [
   *     { value, ts, observationTS, eventId, eventOp },
   *     ...
   *   ]
   * }
   *
   * This is useful for understanding how each field evolved over time
   * and supports proper event sourcing where ALT events may only have partial data.
   * Sorted by ts (sync time) for stable ordering.
   */
  function getFieldHistory(entityId, events) {
    const normalizedEntityId = normalizeDocketNumber(entityId);
    const entityEvents = events
      .filter(e => normalizeDocketNumber(e.entity_id || e.target?.id) === normalizedEntityId)
      // Sort by ts (sync time) for stable ordering
      .sort((a, b) => a.ts - b.ts);

    // Track all versions of each field
    const fieldHistory = {};

    function addFieldVersion(fieldName, value, syncTs, observationTS, eventId, eventOp) {
      // Skip empty values - they don't represent field deletions
      if (value === null || value === undefined || value === '') {
        return;
      }
      if (!fieldHistory[fieldName]) {
        fieldHistory[fieldName] = [];
      }
      fieldHistory[fieldName].push({
        value,
        ts: syncTs,
        observationTS: observationTS,
        ts_iso: new Date(observationTS).toISOString(),
        eventId,
        eventOp
      });
    }

    for (const event of entityEvents) {
      const observationTS = getObservationTS(event);
      const payload = event.payload || event.context || {};

      switch (event.op) {
        case OPERATORS.INS:
          // All fields from INS data
          if (payload.data) {
            for (const [field, value] of Object.entries(payload.data)) {
              if (value !== undefined) {
                addFieldVersion(field, value, event.ts, observationTS, event.id, event.op);
              }
            }
          }
          break;

        case OPERATORS.ALT:
          // ALT with payload.data = full record
          if (payload.data) {
            for (const [field, value] of Object.entries(payload.data)) {
              if (value !== undefined) {
                addFieldVersion(field, value, event.ts, observationTS, event.id, event.op);
              }
            }
          }

          // ALT with payload.changes = delta update
          if (payload.changes) {
            for (const [field, change] of Object.entries(payload.changes)) {
              const newValue = change.new !== undefined ? change.new : change;
              addFieldVersion(field, newValue, event.ts, observationTS, event.id, event.op);
            }
          }

          // ALT with single field update
          if (event.target.field && payload.new !== undefined) {
            addFieldVersion(event.target.field, payload.new, event.ts, observationTS, event.id, event.op);
          }
          break;
      }
    }

    return fieldHistory;
  }

  /**
   * Get the latest value for each field from field history
   * This is the definitive current state based on all available events.
   */
  function getLatestFieldValues(entityId, events) {
    const fieldHistory = getFieldHistory(entityId, events);
    const latestValues = {};

    for (const [field, versions] of Object.entries(fieldHistory)) {
      if (versions.length > 0) {
        // Versions are already sorted by timestamp
        latestValues[field] = versions[versions.length - 1].value;
      }
    }

    return latestValues;
  }

  // =============================================================================
  // QUERY UTILITIES
  // =============================================================================

  /**
   * Filter events by operator
   */
  function filterByOperator(events, op) {
    return events.filter(e => e.op === op);
  }

  /**
   * Filter events by time range
   */
  function filterByTimeRange(events, startTs, endTs) {
    return events.filter(e => e.ts >= startTs && e.ts <= endTs);
  }

  /**
   * Filter events by source table
   */
  function filterByTable(events, tableName) {
    return events.filter(e => {
      const payload = e.payload || e.context || {};
      return payload.table === tableName;
    });
  }

  /**
   * Get all unique entity IDs that have events (normalized for deduplication)
   */
  function getAllEntityIds(events) {
    return [...new Set(events.map(e => normalizeDocketNumber(e.entity_id || e.target?.id)).filter(Boolean))];
  }

  /**
   * Get entities created in a time range (normalized for deduplication)
   */
  function getCreatedInRange(events, startTs, endTs) {
    return [...new Set(
      filterByOperator(events, OPERATORS.INS)
        .filter(e => e.ts >= startTs && e.ts <= endTs)
        .map(e => normalizeDocketNumber(e.entity_id || e.target?.id))
        .filter(Boolean)
    )];
  }

  /**
   * Get entities with specific field changes (normalized for deduplication)
   */
  function getEntitiesWithFieldChange(events, fieldName) {
    return [...new Set(
      filterByOperator(events, OPERATORS.ALT)
        .filter(e => {
          if (e.target?.field === fieldName) return true;
          const payload = e.payload || e.context || {};
          if (payload.changes && payload.changes[fieldName]) return true;
          return false;
        })
        .map(e => normalizeDocketNumber(e.entity_id || e.target?.id))
        .filter(Boolean)
    )];
  }

  // =============================================================================
  // XANO API INTEGRATION
  // =============================================================================

  /**
   * Fetch events from the operations table
   * GET https://xvkq-pq7i-idtl.n7d.xano.io/api:3CsVHkZK/eviction_operations
   */
  async function fetchEvents(options = {}) {
    const params = new URLSearchParams();

    if (options.entity_id) {
      params.append('entity_id', options.entity_id);
    }
    if (options.op) {
      params.append('op', options.op);
    }
    if (options.since_ts) {
      params.append('ts_gt', options.since_ts);
    }
    if (options.since_ts_gte) {
      params.append('ts_gte', options.since_ts_gte);
    }
    if (options.source_table) {
      params.append('source_table', options.source_table);
    }
    if (options.per_page) {
      params.append('per_page', options.per_page);
    }
    if (options.page) {
      params.append('page', options.page);
    }

    const url = params.toString()
      ? `${CONFIG.OPERATIONS_GET_URL}?${params.toString()}`
      : CONFIG.OPERATIONS_GET_URL;

    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Failed to fetch events: ${response.status}`);
    }

    const data = await response.json();

    // Handle paginated response format
    const items = data.items || data;

    // Convert from Xano format (TEXT JSON fields) to internal format
    return Array.isArray(items) ? items.map(fromXanoFormat) : items;
  }

  /**
   * Fetch events from the paginated endpoint
   * Uses the new paginated API that returns metadata about pages
   *
   * Response format:
   * {
   *   itemsReceived: number,
   *   curPage: number,
   *   nextPage: number | null,
   *   prevPage: number | null,
   *   offset: number,
   *   perPage: number,
   *   itemsTotal: number,
   *   pageTotal: number,
   *   items: Event[]
   * }
   *
   * @param {object} options - Fetch options
   * @param {number} options.page - Page number to fetch (default: 1)
   * @param {number} options.perPage - Items per page (default: CONFIG.DEFAULT_PAGE_SIZE)
   * @param {number} options.since_ts - Only fetch events after this timestamp
   * @returns {object} Paginated response with items and metadata
   */
  async function fetchEventsPaginated(options = {}) {
    const params = new URLSearchParams();

    // Page number (1-indexed)
    params.append('page', options.page || 1);

    // Items per page
    params.append('per_page', options.perPage || CONFIG.DEFAULT_PAGE_SIZE);

    // Optional timestamp filter for incremental sync
    if (options.since_ts) {
      params.append('ts_gt', options.since_ts);
    }
    if (options.since_ts_gte) {
      params.append('ts_gte', options.since_ts_gte);
    }

    const url = `${CONFIG.OPERATIONS_PAGINATED_URL}?${params.toString()}`;

    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Failed to fetch paginated events: ${response.status}`);
    }

    const data = await response.json();

    // Convert items from Xano format
    const items = Array.isArray(data.items)
      ? data.items.map(fromXanoFormat)
      : [];

    return {
      items: items,
      itemsReceived: data.itemsReceived || items.length,
      curPage: data.curPage || 1,
      nextPage: data.nextPage || null,
      prevPage: data.prevPage || null,
      offset: data.offset || 0,
      perPage: data.perPage || CONFIG.DEFAULT_PAGE_SIZE,
      itemsTotal: data.itemsTotal || items.length,
      pageTotal: data.pageTotal || 1
    };
  }

  /**
   * Fetch all events using the paginated endpoint
   * Iterates through all pages efficiently
   *
   * @param {object} options - Fetch options
   * @param {function} options.onProgress - Progress callback ({ page, pageTotal, itemsFetched, itemsTotal })
   * @param {function} options.log - Logging function
   * @param {number} options.perPage - Items per page
   * @param {number} options.since_ts - Only fetch events after this timestamp
   * @param {number} options.maxPages - Maximum pages to fetch (safety limit, default: 100)
   * @returns {object} { events, eventCount, pagesFetched }
   */
  async function fetchAllEventsPaginated(options = {}) {
    const log = options.log || console.log;
    const onProgress = options.onProgress || null;
    const perPage = options.perPage || CONFIG.DEFAULT_PAGE_SIZE;
    const maxPages = options.maxPages || 100;

    log('🚀 Fetching all EO events (paginated)...');
    log('================================');

    let allEvents = [];
    let page = 1;
    let pageTotal = null;
    let itemsTotal = null;

    while (true) {
      const result = await fetchEventsPaginated({
        page,
        perPage,
        since_ts: options.since_ts,
        since_ts_gte: options.since_ts_gte
      });

      // First page gives us total counts
      if (page === 1) {
        pageTotal = result.pageTotal;
        itemsTotal = result.itemsTotal;
        log(`  Total items: ${itemsTotal}, Total pages: ${pageTotal}`);
      }

      allEvents = allEvents.concat(result.items);
      log(`  Fetched page ${page}/${pageTotal}: ${result.itemsReceived} events (total: ${allEvents.length})`);

      if (onProgress) {
        onProgress({
          page,
          pageTotal: result.pageTotal,
          itemsFetched: allEvents.length,
          itemsTotal: result.itemsTotal
        });
      }

      // Check if we've reached the last page
      if (result.nextPage === null || result.items.length === 0) {
        break;
      }

      // Safety limit
      if (page >= maxPages) {
        log(`  ⚠️ Reached page limit (${maxPages}), stopping fetch`);
        break;
      }

      page++;
    }

    log(`\n✅ Fetched ${allEvents.length} total events across ${page} pages`);

    // Analyze event distribution
    const opCounts = {};
    for (const event of allEvents) {
      opCounts[event.op] = (opCounts[event.op] || 0) + 1;
    }
    log('\n  Event distribution:');
    for (const [op, count] of Object.entries(opCounts)) {
      log(`    ${op}: ${count} events`);
    }

    // Reconstruct state
    log('\n🔍 Reconstructing state from events...');
    const reconstructed = reconstructAllStates(allEvents);
    log(`  Reconstructed ${reconstructed.length} entities`);

    log('\n================================');
    log('🎉 Fetch complete!');

    return {
      events: allEvents,
      eventCount: allEvents.length,
      opCounts: opCounts,
      states: reconstructed,
      stateCount: reconstructed.length,
      pagesFetched: page,
      itemsTotal: itemsTotal
    };
  }

  /**
   * Push a single event to the operations table
   * Requires authentication - POST endpoint must be set via setPostEndpoint()
   */
  async function pushEvent(event) {
    if (!isPostEndpointAvailable()) {
      throw new Error('POST endpoint not available. Authentication required.');
    }

    // Convert to Xano format (stringify JSON fields)
    const xanoEvent = toXanoFormat(event);

    const response = await fetch(CONFIG.OPERATIONS_POST_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(xanoEvent)
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Failed to push event: ${response.status} - ${errorText}`);
    }

    return response.json();
  }

  /**
   * Push multiple events in batches
   */
  async function pushEventsBatch(events, onProgress = null) {
    const results = {
      success: 0,
      failed: 0,
      errors: []
    };

    for (let i = 0; i < events.length; i += CONFIG.BATCH_SIZE) {
      const batch = events.slice(i, i + CONFIG.BATCH_SIZE);

      for (const event of batch) {
        try {
          await pushEvent(event);
          results.success++;
        } catch (error) {
          results.failed++;
          results.errors.push({ event: event.id, error: error.message });
        }
      }

      if (onProgress) {
        onProgress({
          processed: Math.min(i + CONFIG.BATCH_SIZE, events.length),
          total: events.length,
          success: results.success,
          failed: results.failed
        });
      }

      // Small delay between batches to avoid rate limiting
      if (i + CONFIG.BATCH_SIZE < events.length) {
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    }

    return results;
  }


  // =============================================================================
  // FULL SYNC WORKFLOW (EO-ONLY)
  // =============================================================================

  /**
   * Fetch all events from EO operations table and reconstruct state
   */
  async function fetchAllEvents(options = {}) {
    const log = options.log || console.log;
    const onProgress = options.onProgress || null;

    log('🚀 Fetching all EO events...');
    log('================================');

    let allEvents = [];
    let page = 1;
    let hasMore = true;

    while (hasMore) {
      const events = await fetchEvents({ page, per_page: 1000 });
      const data = Array.isArray(events) ? events : (events.items || events.data || []);

      if (data.length === 0) {
        hasMore = false;
      } else {
        allEvents = allEvents.concat(data);
        log(`  Fetched page ${page}: ${data.length} events (total: ${allEvents.length})`);
        page++;

        if (onProgress) {
          onProgress({ page, count: allEvents.length });
        }

        // Safety limit
        if (page > 100) {
          log('  ⚠️ Reached page limit (100), stopping fetch');
          hasMore = false;
        }
      }
    }

    log(`\n✅ Fetched ${allEvents.length} total events`);

    // Analyze event distribution
    const opCounts = {};
    for (const event of allEvents) {
      opCounts[event.op] = (opCounts[event.op] || 0) + 1;
    }
    log('\n  Event distribution:');
    for (const [op, count] of Object.entries(opCounts)) {
      log(`    ${op}: ${count} events`);
    }

    // Reconstruct state
    log('\n🔍 Reconstructing state from events...');
    const reconstructed = reconstructAllStates(allEvents);
    log(`  Reconstructed ${reconstructed.length} entities`);

    log('\n================================');
    log('🎉 Fetch complete!');

    return {
      events: allEvents,
      eventCount: allEvents.length,
      opCounts: opCounts,
      states: reconstructed,
      stateCount: reconstructed.length
    };
  }

  /**
   * Validate EO state by fetching and reconstructing
   */
  async function validateEOState(sampleSize = 100) {
    console.log('🧪 Validating EO State');
    console.log('==================================\n');

    // Fetch events
    const events = await fetchEvents({ per_page: sampleSize });
    const data = Array.isArray(events) ? events : (events.items || events.data || []);

    console.log(`Fetched ${data.length} events`);

    // Show sample events
    console.log('\nSample events:');
    for (const event of data.slice(0, 5)) {
      console.log(JSON.stringify(event, null, 2));
      console.log('---');
    }

    // Reconstruct and validate
    const states = reconstructAllStates(data);
    console.log(`\nReconstructed ${states.length} entity states`);

    // Show sample reconstructed state
    if (states.length > 0) {
      console.log('\nSample reconstructed state:');
      console.log(JSON.stringify(states[0], null, 2));
    }

    return {
      eventCount: data.length,
      stateCount: states.length,
      events: data,
      states: states
    };
  }

  // =============================================================================
  // CURRENT STATE API (new model)
  // =============================================================================

  /**
   * Current State table schema:
   * {
   *   id: integer (auto-increment PK),
   *   created_at: timestamp,
   *   object_type: text (e.g., 'eviction_case'),
   *   object_id: text (docket number),
   *   data: json (array of state entries — enumerable payload for replay),
   *   updated_at: timestamp,
   *   deleted: boolean (soft-delete flag)
   * }
   *
   * The `data` field is a JSON array. Each entry is a snapshot of the case at
   * a point in time. New updates append entries to the array. Current state is
   * derived by merging all entries cumulatively (same semantics as EO replay).
   */

  /**
   * Parse a current-state row into internal case format.
   * Replays the `stateData` array entries cumulatively to produce current state.
   *
   * New API schema (eviction_current_state):
   *   { id, created_at, docket_number, updated, stateData, fileDate }
   *
   * @param {object} row - A row from the eviction_current_state table
   * @returns {object} Reconstructed case state
   */
  function fromCurrentStateFormat(row) {
    // Support new field name (docket_number) with fallback to legacy (object_id)
    const objectId = row.docket_number || row.object_id || '';
    const normalizedId = normalizeDocketNumber(objectId);

    // Parse the stateData field — it may be a JSON string or already parsed
    // Support new field name (stateData) with fallback to legacy (data)
    let dataEntries = row.stateData !== undefined ? row.stateData : row.data;
    if (typeof dataEntries === 'string') {
      dataEntries = safeJsonParse(dataEntries, []);
    }
    if (!Array.isArray(dataEntries)) {
      dataEntries = dataEntries ? [dataEntries] : [];
    }

    // Replay entries cumulatively to build current state
    const state = {};

    function isEmpty(value) {
      if (value === null || value === undefined || value === '') return true;
      if (Array.isArray(value) && value.length === 0) return true;
      return false;
    }

    for (const entry of dataEntries) {
      if (!entry || typeof entry !== 'object') continue;
      for (const [field, value] of Object.entries(entry)) {
        if (!isEmpty(value)) {
          state[field] = value;
        }
      }
    }

    // Ensure identity fields are set
    state.Docket_Number = state.Docket_Number || normalizedId;
    state._entity_id = normalizedId;
    // Support new field name (updated) with fallback to legacy (updated_at)
    const updatedTs = row.updated || row.updated_at;
    state._last_updated = updatedTs
      ? (typeof updatedTs === 'number' ? updatedTs : new Date(updatedTs).getTime())
      : (row.created_at ? (typeof row.created_at === 'number' ? row.created_at : new Date(row.created_at).getTime()) : Date.now());
    state._current_state_id = row.id;
    state._data_entries_count = dataEntries.length;
    // fileDate from the new API schema
    if (row.fileDate) {
      state.File_Date = state.File_Date || row.fileDate;
    }

    return state;
  }

  /**
   * Convert internal case data to current-state POST format.
   *
   * New API schema: { docket_number, stateData, fileDate }
   *
   * @param {string} objectId - The docket number
   * @param {object|Array} data - Case data entry or array of entries for the `stateData` field
   * @param {object} options - Additional options
   * @param {string} options.fileDate - File date for the case (YYYY-MM-DD)
   * @returns {object} Body ready for POST to /eviction_current_state
   */
  function toCurrentStateFormat(objectId, data, options = {}) {
    const normalizedId = normalizeDocketNumber(objectId);
    const dataArray = Array.isArray(data) ? data : [data];

    const body = {
      docket_number: normalizedId,
      stateData: dataArray
    };

    // Include fileDate if provided or derivable from data
    if (options.fileDate) {
      body.fileDate = options.fileDate;
    } else {
      // Try to derive from data entries
      const fileDate = dataArray[0]?.File_Date || dataArray[0]?.file_date;
      if (fileDate) body.fileDate = fileDate;
    }

    return body;
  }

  /**
   * Fetch records from the current-state endpoint.
   * The API returns a paginated list sorted by fileDate desc (2500 per page default).
   * No server-side filtering is supported — only pagination params.
   *
   * @param {object} options - Fetch options
   * @param {number} options.page - Page number
   * @param {number} options.per_page - Items per page
   * @returns {Array} Array of current-state rows
   */
  async function fetchCurrentState(options = {}) {
    const params = new URLSearchParams();

    if (options.page) {
      params.append('page', options.page);
    }
    if (options.per_page) {
      params.append('per_page', options.per_page);
    }
    if (options.object_id) {
      params.append('object_id', options.object_id);
    }

    const url = params.toString()
      ? `${CONFIG.CURRENT_STATE_GET_URL}?${params.toString()}`
      : CONFIG.CURRENT_STATE_GET_URL;

    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Failed to fetch current state: ${response.status}`);
    }

    return await response.json();
  }

  /**
   * Fetch all current-state records with pagination.
   * Iterates through all pages and reconstructs case states.
   *
   * @param {object} options - Fetch options
   * @param {function} options.onProgress - Progress callback ({ page, pageTotal, itemsFetched, itemsTotal })
   * @param {function} options.log - Logging function
   * @param {number} options.perPage - Items per page (default: CONFIG.DEFAULT_PAGE_SIZE)
   * @param {number} options.maxPages - Safety limit (default: 100)
   * @returns {object} { rows, states, stateCount, pagesFetched, itemsTotal }
   */
  async function fetchAllCurrentState(options = {}) {
    const log = options.log || console.log;
    const onProgress = options.onProgress || null;
    const perPage = options.perPage || CONFIG.DEFAULT_PAGE_SIZE;
    const maxPages = options.maxPages || 100;

    log('Fetching all current-state records...');

    let allRows = [];
    let page = 1;
    let pageTotal = null;
    let itemsTotal = null;

    while (true) {
      const fetchOpts = {
        page,
        per_page: perPage
      };

      const result = await fetchCurrentState(fetchOpts);

      // Handle paginated (object with items/nextPage) vs flat array response
      if (Array.isArray(result)) {
        // Flat array response (no pagination metadata available)
        allRows = allRows.concat(result);

        if (onProgress) {
          onProgress({
            page,
            pageTotal: pageTotal || page,
            itemsFetched: allRows.length,
            itemsTotal: itemsTotal || allRows.length
          });
        }

        // If we got fewer items than perPage, we've reached the end
        if (result.length < perPage) {
          break;
        }
      } else {
        // Paginated response with metadata (Xano format: items, nextPage, curPage, pageTotal, itemsTotal)
        const items = result.items || [];
        allRows = allRows.concat(items);

        if (page === 1) {
          pageTotal = result.pageTotal || 1;
          itemsTotal = result.itemsTotal || items.length;
        }

        if (onProgress) {
          onProgress({
            page,
            pageTotal: result.pageTotal || page,
            itemsFetched: allRows.length,
            itemsTotal: result.itemsTotal || allRows.length
          });
        }

        // Stop when Xano signals no next page, or we got an empty page,
        // or we've reached the known page total
        if (result.nextPage === null || result.nextPage === undefined || items.length === 0) {
          break;
        }
        if (pageTotal && page >= pageTotal) {
          break;
        }
      }

      if (page >= maxPages) {
        log('Reached page limit (' + maxPages + '), stopping fetch');
        break;
      }

      page++;
    }

    log('Fetched ' + allRows.length + ' current-state rows across ' + page + ' pages');

    // Convert rows to internal case states
    const states = [];
    for (const row of allRows) {
      const state = fromCurrentStateFormat(row);
      if (state && state._entity_id) {
        states.push(state);
      }
    }

    log('Reconstructed ' + states.length + ' entities from current-state table');

    return {
      rows: allRows,
      states: states,
      stateCount: states.length,
      pagesFetched: page,
      itemsTotal: itemsTotal || allRows.length
    };
  }

  /**
   * Push a record to the current-state endpoint.
   *
   * @param {object} body - The POST body (from toCurrentStateFormat)
   * @returns {object} API response
   */
  async function pushCurrentState(body) {
    const response = await fetch(CONFIG.CURRENT_STATE_POST_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Failed to push current state: ${response.status} - ${errorText}`);
    }

    return response.json();
  }

  /**
   * Push multiple current-state records in batches.
   *
   * @param {Array} records - Array of POST bodies (from toCurrentStateFormat)
   * @param {function} onProgress - Progress callback
   * @returns {object} { success, failed, errors }
   */
  async function pushCurrentStateBatch(records, onProgress = null) {
    const results = { success: 0, failed: 0, errors: [] };

    for (let i = 0; i < records.length; i += CONFIG.BATCH_SIZE) {
      const batch = records.slice(i, i + CONFIG.BATCH_SIZE);

      for (const record of batch) {
        try {
          await pushCurrentState(record);
          results.success++;
        } catch (error) {
          results.failed++;
          results.errors.push({ docket_number: record.docket_number || record.object_id, error: error.message });
        }
      }

      if (onProgress) {
        onProgress({
          processed: Math.min(i + CONFIG.BATCH_SIZE, records.length),
          total: records.length,
          success: results.success,
          failed: results.failed
        });
      }

      // Small delay between batches
      if (i + CONFIG.BATCH_SIZE < records.length) {
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    }

    return results;
  }

  // =============================================================================
  // APPEND-ONLY STATE UPDATE API
  // =============================================================================

  /**
   * Build a state_data entry with metadata for the append-only update endpoint.
   *
   * Each entry in the stateData array includes operation metadata alongside the
   * case data fields:
   *   { op, ts, source, ...caseFields }
   *
   * @param {object} caseData - The case record fields (e.g., Status, Office, Attorney, etc.)
   * @param {object} options
   * @param {string} options.op - Operation type: "INS" for new cases, "ALT" for updates (default: "INS")
   * @param {string} options.source - Source identifier (default: "bulk_upload")
   * @param {string} options.ts - ISO timestamp override (default: now)
   * @returns {object} state_data object ready for the update endpoint
   */
  function buildStateEntry(caseData, options = {}) {
    const op = options.op || OPERATORS.INS;
    const source = options.source || 'bulk_upload';
    const ts = options.ts || new Date().toISOString();

    // Merge metadata with case data; metadata fields come first for readability
    return {
      op,
      ts,
      source,
      ...caseData
    };
  }

  /**
   * Build the POST body for the update_eviction_state endpoint.
   *
   * @param {string} docketNumber - The docket number
   * @param {object} stateEntry - A state_data object (from buildStateEntry or raw)
   * @param {object} options
   * @param {string} options.fileDate - Filing date in YYYY-MM-DD format
   * @param {number|string} options.updatedAt - Timestamp for the update
   * @returns {object} POST body for update_eviction_state
   */
  function toUpdateStatePayload(docketNumber, stateEntry, options = {}) {
    const normalizedId = normalizeDocketNumber(docketNumber);

    const body = {
      docket_number: normalizedId,
      state_data: stateEntry
    };

    if (options.updatedAt) {
      body.updated_at = typeof options.updatedAt === 'number'
        ? new Date(options.updatedAt).toISOString()
        : options.updatedAt;
    }

    // Derive file_date from options or from the state entry
    const fileDate = options.fileDate
      || stateEntry.File_Date
      || stateEntry.file_date;
    if (fileDate) {
      body.file_date = fileDate;
    }

    return body;
  }

  /**
   * Push a single state update via the append-only endpoint.
   * Upserts by docket_number: appends state_data to existing stateData array,
   * or creates a new row if docket_number doesn't exist.
   *
   * @param {object} body - POST body from toUpdateStatePayload()
   * @returns {object} API response
   */
  async function pushStateUpdate(body) {
    const response = await fetch(CONFIG.UPDATE_STATE_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Failed to push state update: ${response.status} - ${errorText}`);
    }

    return response.json();
  }

  /**
   * Push multiple state updates in batches via the append-only endpoint.
   *
   * @param {Array} payloads - Array of POST bodies (from toUpdateStatePayload)
   * @param {function} onProgress - Progress callback
   * @returns {object} { success, failed, errors }
   */
  async function pushStateUpdateBatch(payloads, onProgress = null) {
    const results = { success: 0, failed: 0, errors: [] };

    for (let i = 0; i < payloads.length; i += CONFIG.BATCH_SIZE) {
      const batch = payloads.slice(i, i + CONFIG.BATCH_SIZE);

      for (const payload of batch) {
        try {
          await pushStateUpdate(payload);
          results.success++;
        } catch (error) {
          results.failed++;
          results.errors.push({ docket_number: payload.docket_number, error: error.message });
        }
      }

      if (onProgress) {
        onProgress({
          processed: Math.min(i + CONFIG.BATCH_SIZE, payloads.length),
          total: payloads.length,
          success: results.success,
          failed: results.failed
        });
      }

      // Small delay between batches
      if (i + CONFIG.BATCH_SIZE < payloads.length) {
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    }

    return results;
  }

  /**
   * Get the data entries (update history) for a specific record from the current-state table.
   * Useful for audit/replay of a single entity.
   *
   * @param {string} objectId - The docket number
   * @returns {Array} The data entries array from the record
   */
  async function fetchCurrentStateHistory(objectId) {
    const result = await fetchCurrentState({ object_id: normalizeDocketNumber(objectId) });
    // Handle both paginated (object with items) and flat array responses
    const rows = Array.isArray(result) ? result : (result.items || []);
    if (rows.length === 0) return [];

    const row = rows[0];
    let dataEntries = row.stateData !== undefined ? row.stateData : row.data;
    if (typeof dataEntries === 'string') {
      dataEntries = safeJsonParse(dataEntries, []);
    }
    return Array.isArray(dataEntries) ? dataEntries : [dataEntries];
  }

  // =============================================================================
  // XANO TABLE SCHEMA (for reference when creating table)
  // =============================================================================

  const XANO_TABLE_SCHEMA = {
    tableName: 'eviction_operations',
    fields: [
      { name: 'id', type: 'text', required: true, primary: true },
      { name: 'ts', type: 'integer', required: true, index: true }, // Unix timestamp ms - when event was synced
      { name: 'op', type: 'text', required: true, index: true }, // INS, ALT
      { name: 'target', type: 'json', required: true },
      { name: 'payload', type: 'json', required: true }, // Contains observationTS, table, data/changes
      { name: 'frame', type: 'json', required: false },
      // Computed/extracted fields for indexing
      { name: 'source_table', type: 'text', computed: 'payload.table', index: true },
      { name: 'entity_id', type: 'text', computed: 'target.id', index: true }
    ],
    indexes: [
      { fields: ['entity_id', 'ts'], name: 'idx_entity_timeline' },
      { fields: ['op', 'ts'], name: 'idx_op_timeline' },
      { fields: ['source_table', 'ts'], name: 'idx_table_timeline' }
    ],
    // Note: ts is the time the event was synced to the database
    // Events are replayed in ts order for simple, stable cumulative state building
    notes: 'Events replayed in ts order; fields accumulate cumulatively, nulls never overwrite'
  };

  // =============================================================================
  // PUBLIC API
  // =============================================================================

  return {
    // Configuration
    CONFIG,
    OPERATORS,
    OPERATOR_MEANINGS,

    // Authentication (POST endpoint requires login)
    setPostEndpoint,
    isPostEndpointAvailable,

    // Event creation
    createEvent,
    createInsertEvent,
    createUpdateEvent,
    createBulkUpdateEvent,

    // Data normalization
    normalizeDocketNumber,
    normalizeCase,
    diffCases,

    // Migration/Sync
    convertLegacyActivityToEvents,
    convertSnapshotToEvents,
    fetchAllEvents,
    fetchAllEventsPaginated,
    validateEOState,

    // State reconstruction
    reconstructEntityState,
    reconstructAllStates,
    getAuditTrail,
    getOperatorPattern,
    getFieldHistory,
    getLatestFieldValues,
    getObservationTS,

    // Query utilities
    filterByOperator,
    filterByTimeRange,
    filterByTable,
    getAllEntityIds,
    getCreatedInRange,
    getEntitiesWithFieldChange,

    // Xano API (legacy event-sourcing)
    fetchEvents,
    fetchEventsPaginated,
    pushEvent,
    pushEventsBatch,

    // Current State API (new model)
    fetchCurrentState,
    fetchAllCurrentState,
    pushCurrentState,
    pushCurrentStateBatch,
    fetchCurrentStateHistory,

    // Append-only state update API
    buildStateEntry,
    toUpdateStatePayload,
    pushStateUpdate,
    pushStateUpdateBatch,

    // Format conversion
    toXanoFormat,
    fromXanoFormat,
    fromCurrentStateFormat,
    toCurrentStateFormat,

    // Schema reference
    XANO_TABLE_SCHEMA,

    // Utility
    generateUUID
  };
})();

// Export for Node.js environments
if (typeof module !== 'undefined' && module.exports) {
  module.exports = EOMigration;
}

// Usage examples (commented out for reference):
/*

// Fetch all events and reconstruct state
EOMigration.fetchAllEvents().then(result => {
  console.log('Fetched events:', result.eventCount);
  console.log('Reconstructed states:', result.stateCount);
});

// Validate EO state with sample
EOMigration.validateEOState(50).then(result => {
  console.log('Validation complete:', result);
});

// Create a new event for a case update
const updateEvent = EOMigration.createUpdateEvent(
  '24GC1234',           // docket number
  'Status',             // field name
  'Active',             // old value
  'Disposed',           // new value
  { actor: 'system' }   // frame (optional)
);
console.log('Update event:', updateEvent);

// Push event to EO operations table
await EOMigration.pushEvent(updateEvent);

// Reconstruct state from events
const events = await EOMigration.fetchEvents({ entity_id: '24GC1234' });
const state = EOMigration.reconstructEntityState('24GC1234', events);
console.log('Current state:', state);

// Get audit trail
const trail = EOMigration.getAuditTrail('24GC1234', events);
console.log('Audit trail:', trail);

*/
