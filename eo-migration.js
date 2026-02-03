/**
 * EO (Epistemic Operators) Migration Script
 *
 * Migrates Nashville eviction tracker data to an append-only event-sourced database
 * using the Epistemic Operators grammar.
 *
 * Operations Table Schema (Xano: evictionsEOevents):
 * - id: UUID (primary key)
 * - ts: INTEGER (Unix timestamp in milliseconds)
 * - ts_iso: TIMESTAMP (ISO formatted timestamp)
 * - op: TEXT (INS, DES, SEG, CON, SYN, ALT, SUP, REC, NUL)
 * - entity_id: TEXT (case docket number)
 * - entity_type: TEXT (e.g., 'eviction_case')
 * - source_table: TEXT (e.g., 'eviction_cases')
 * - target: TEXT (JSON stringified - what's being operated on)
 * - context: TEXT (JSON stringified - where/how it's happening)
 * - frame: TEXT (JSON stringified - optional interpretive context)
 */

const EOMigration = (function() {
  'use strict';

  // =============================================================================
  // CONFIGURATION
  // =============================================================================

  const CONFIG = {
    // EO operations endpoints (primary API)
    OPERATIONS_GET_URL: 'https://xvkq-pq7i-idtl.n7d.xano.io/api:3CsVHkZK/eviction_operations',
    // POST URL is encrypted and must be set via setPostEndpoint() after authentication
    OPERATIONS_POST_URL: null,

    // Source table identifier
    SOURCE_TABLE: 'eviction_cases',

    // Batch size for migrations
    BATCH_SIZE: 100
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
   * The nine Epistemic Operators
   */
  const OPERATORS = {
    NUL: 'NUL',  // Recognizing absence/deletion
    DES: 'DES',  // Naming/defining (designation)
    INS: 'INS',  // Creating/instantiating
    SEG: 'SEG',  // Segmenting/filtering
    CON: 'CON',  // Connecting/relating
    ALT: 'ALT',  // Alternating/transitioning state
    SYN: 'SYN',  // Synthesizing/merging
    SUP: 'SUP',  // Superposing/layering context
    REC: 'REC'   // Reconfiguring/learning
  };

  /**
   * Operator semantics for eviction filing tracking
   */
  const OPERATOR_MEANINGS = {
    INS: 'New eviction filing recorded',
    ALT: 'Filing data updated',
    NUL: 'Filing deleted/archived',
    CON: 'Filings linked (same property/plaintiff)',
    SYN: 'Filings merged (duplicates)',
    SEG: 'Filing filtered/categorized',
    SUP: 'Context overlay added',
    DES: 'Field/entity defined',
    REC: 'Pattern learned/reconfigured'
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
  // EVENT CREATION
  // =============================================================================

  /**
   * Create a base EO event structure (internal format)
   */
  function createEvent(op, target, context, frame = null) {
    const ts = Date.now();
    const event = {
      id: generateUUID(),
      ts: ts,
      ts_iso: new Date(ts).toISOString(),
      op: op,
      entity_id: target.id || '',
      entity_type: target.type || 'eviction_case',
      source_table: context.table || CONFIG.SOURCE_TABLE,
      target: target,
      context: context,
      frame: frame || {}
    };

    return event;
  }

  /**
   * Convert an event to Xano table format (TEXT columns for JSON fields)
   */
  function toXanoFormat(event) {
    return {
      id: event.id,
      ts: event.ts,
      ts_iso: event.ts_iso,
      op: event.op,
      entity_id: event.entity_id,
      entity_type: event.entity_type,
      source_table: event.source_table,
      target: JSON.stringify(event.target),
      context: JSON.stringify(event.context),
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
   */
  function fromXanoFormat(record) {
    return {
      id: record.id,
      ts: record.ts,
      ts_iso: record.ts_iso,
      op: record.op,
      entity_id: record.entity_id,
      entity_type: record.entity_type,
      source_table: record.source_table,
      target: safeJsonParse(record.target, {}),
      context: safeJsonParse(record.context, {}),
      frame: safeJsonParse(record.frame, {})
    };
  }

  /**
   * Create an INS (insert/create) event for a new eviction filing
   */
  function createInsertEvent(caseData, frame = null) {
    const docketNumber = caseData.Docket_Number || caseData.docket_number;

    if (!docketNumber) {
      throw new Error('Cannot create INS event: missing docket number');
    }

    return createEvent(
      OPERATORS.INS,
      {
        id: docketNumber,
        type: 'eviction_case'
      },
      {
        table: CONFIG.SOURCE_TABLE,
        data: normalizeCase(caseData)
      },
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
    return createEvent(
      OPERATORS.ALT,
      {
        id: docketNumber,
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
   */
  function createBulkUpdateEvent(docketNumber, changes, frame = null) {
    // changes = { fieldName: { old: oldValue, new: newValue }, ... }
    return createEvent(
      OPERATORS.ALT,
      {
        id: docketNumber,
        type: 'eviction_case'
      },
      {
        table: CONFIG.SOURCE_TABLE,
        changes: changes
      },
      frame || {
        source: 'bulk_update',
        version: '1.0'
      }
    );
  }

  /**
   * Create a NUL (delete/nullify) event
   */
  function createDeleteEvent(docketNumber, reason = null, frame = null) {
    return createEvent(
      OPERATORS.NUL,
      {
        id: docketNumber
      },
      {
        table: CONFIG.SOURCE_TABLE,
        reason: reason || 'deleted'
      },
      frame || {
        source: 'deletion',
        reversible: true
      }
    );
  }

  /**
   * Create a CON (connect) event for linking related cases
   */
  function createConnectionEvent(docketNumber1, docketNumber2, relationshipType, frame = null) {
    return createEvent(
      OPERATORS.CON,
      {
        id: docketNumber1,
        type: 'eviction_case'
      },
      {
        table: CONFIG.SOURCE_TABLE,
        related: docketNumber2,
        relationship: relationshipType // e.g., 'same_property', 'same_plaintiff', 'same_defendant'
      },
      frame || {
        source: 'link',
        version: '1.0'
      }
    );
  }

  /**
   * Create a SYN (synthesize/merge) event for merging duplicate cases
   */
  function createMergeEvent(primaryDocket, mergedDockets, frame = null) {
    return createEvent(
      OPERATORS.SYN,
      {
        id: primaryDocket,
        type: 'eviction_case'
      },
      {
        table: CONFIG.SOURCE_TABLE,
        merged: mergedDockets, // Array of docket numbers being merged
        action: 'merge_duplicates'
      },
      frame || {
        source: 'deduplication',
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
          context: {
            table: CONFIG.SOURCE_TABLE,
            data: normalizeCase(record)
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
            context: {
              table: CONFIG.SOURCE_TABLE,
              changes: changes
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
        context: {
          table: CONFIG.SOURCE_TABLE,
          data: normalizeCase(record)
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
   * Proper event sourcing implementation:
   * - Handles ALT events even without a prior INS event
   * - Tracks the latest version of each field based on event timestamps
   * - Supports partial updates (deltas) that only contain changed fields
   */
  function reconstructEntityState(entityId, events) {
    const entityEvents = events
      .filter(e => (e.entity_id || e.target?.id) === entityId)
      .sort((a, b) => a.ts - b.ts);

    if (entityEvents.length === 0) {
      return null;
    }

    let state = null;
    let isDeleted = false;
    // Track field versions: { fieldName: { value, ts } }
    let fieldVersions = {};

    /**
     * Update a field only if this event is newer than the last update
     */
    function updateField(fieldName, value, eventTs) {
      if (!fieldVersions[fieldName] || eventTs >= fieldVersions[fieldName].ts) {
        fieldVersions[fieldName] = { value, ts: eventTs };
      }
    }

    /**
     * Apply all fields from a data object
     */
    function applyData(data, eventTs) {
      if (!data) return;
      for (const [field, value] of Object.entries(data)) {
        if (value !== undefined) {
          updateField(field, value, eventTs);
        }
      }
    }

    for (const event of entityEvents) {
      switch (event.op) {
        case OPERATORS.INS:
          // Initialize/update state from INS event data
          applyData(event.context.data, event.ts);
          isDeleted = false;
          break;

        case OPERATORS.ALT:
          // ALT with context.data = full record upsert
          if (event.context.data) {
            applyData(event.context.data, event.ts);
            isDeleted = false;
          }

          // ALT with context.changes = delta update (works even without prior INS)
          if (event.context.changes) {
            for (const [field, change] of Object.entries(event.context.changes)) {
              // change can be { old, new } or just a new value
              const newValue = change.new !== undefined ? change.new : change;
              updateField(field, newValue, event.ts);
            }
          }

          // ALT with single field update via target.field
          if (event.target.field && event.context.new !== undefined) {
            updateField(event.target.field, event.context.new, event.ts);
          }
          break;

        case OPERATORS.NUL:
          isDeleted = true;
          break;

        case OPERATORS.SYN:
          // Mark as merged if this entity was absorbed
          if (event.context?.merged && event.context.merged.includes(entityId)) {
            updateField('_merged_into', event.entity_id || event.target?.id, event.ts);
          }
          break;
      }
    }

    if (isDeleted) {
      return null;
    }

    // Build final state from latest field versions
    if (Object.keys(fieldVersions).length > 0) {
      state = {};
      for (const [field, versionInfo] of Object.entries(fieldVersions)) {
        state[field] = versionInfo.value;
      }

      // Ensure Docket_Number is set
      if (!state.Docket_Number) {
        state.Docket_Number = entityId;
      }
    }

    // Add metadata
    if (state) {
      state._entity_id = entityId;
      state._last_updated = entityEvents[entityEvents.length - 1].ts;
      state._event_count = entityEvents.length;
    }

    return state;
  }

  /**
   * Reconstruct current state of all entities
   */
  function reconstructAllStates(events) {
    // Group events by entity ID
    const byEntity = new Map();

    for (const event of events) {
      // Handle events with missing or null target - use entity_id as fallback
      const entityId = event.entity_id || event.target?.id;
      if (!entityId) {
        // Skip events without a valid entity ID
        continue;
      }
      if (!byEntity.has(entityId)) {
        byEntity.set(entityId, []);
      }
      byEntity.get(entityId).push(event);
    }

    // Reconstruct each entity
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
    return events
      .filter(e => (e.entity_id || e.target?.id) === entityId)
      .sort((a, b) => a.ts - b.ts)
      .map(e => ({
        id: e.id,
        when: new Date(e.ts).toISOString(),
        op: e.op,
        meaning: OPERATOR_MEANINGS[e.op] || e.op,
        details: e.context,
        frame: e.frame
      }));
  }

  /**
   * Get the operator pattern for an entity (for causal analysis)
   */
  function getOperatorPattern(entityId, events) {
    const ops = events
      .filter(e => (e.entity_id || e.target?.id) === entityId)
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
   *     { value, ts, eventId, eventOp },
   *     ...
   *   ]
   * }
   *
   * This is useful for understanding how each field evolved over time
   * and supports proper event sourcing where ALT events may only have partial data.
   */
  function getFieldHistory(entityId, events) {
    const entityEvents = events
      .filter(e => (e.entity_id || e.target?.id) === entityId)
      .sort((a, b) => a.ts - b.ts);

    // Track all versions of each field
    const fieldHistory = {};

    function addFieldVersion(fieldName, value, eventTs, eventId, eventOp) {
      if (!fieldHistory[fieldName]) {
        fieldHistory[fieldName] = [];
      }
      fieldHistory[fieldName].push({
        value,
        ts: eventTs,
        ts_iso: new Date(eventTs).toISOString(),
        eventId,
        eventOp
      });
    }

    for (const event of entityEvents) {
      switch (event.op) {
        case OPERATORS.INS:
          // All fields from INS data
          if (event.context.data) {
            for (const [field, value] of Object.entries(event.context.data)) {
              if (value !== undefined) {
                addFieldVersion(field, value, event.ts, event.id, event.op);
              }
            }
          }
          break;

        case OPERATORS.ALT:
          // ALT with context.data = full record
          if (event.context.data) {
            for (const [field, value] of Object.entries(event.context.data)) {
              if (value !== undefined) {
                addFieldVersion(field, value, event.ts, event.id, event.op);
              }
            }
          }

          // ALT with context.changes = delta update
          if (event.context.changes) {
            for (const [field, change] of Object.entries(event.context.changes)) {
              const newValue = change.new !== undefined ? change.new : change;
              addFieldVersion(field, newValue, event.ts, event.id, event.op);
            }
          }

          // ALT with single field update
          if (event.target.field && event.context.new !== undefined) {
            addFieldVersion(event.target.field, event.context.new, event.ts, event.id, event.op);
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
    return events.filter(e => e.context.table === tableName);
  }

  /**
   * Get all entity IDs that have events
   */
  function getAllEntityIds(events) {
    return [...new Set(events.map(e => e.entity_id || e.target?.id).filter(Boolean))];
  }

  /**
   * Get entities created in a time range
   */
  function getCreatedInRange(events, startTs, endTs) {
    return filterByOperator(events, OPERATORS.INS)
      .filter(e => e.ts >= startTs && e.ts <= endTs)
      .map(e => e.entity_id || e.target?.id)
      .filter(Boolean);
  }

  /**
   * Get entities with specific field changes
   */
  function getEntitiesWithFieldChange(events, fieldName) {
    return filterByOperator(events, OPERATORS.ALT)
      .filter(e => {
        if (e.target?.field === fieldName) return true;
        if (e.context?.changes && e.context.changes[fieldName]) return true;
        return false;
      })
      .map(e => e.entity_id || e.target?.id)
      .filter(Boolean);
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
  // XANO TABLE SCHEMA (for reference when creating table)
  // =============================================================================

  const XANO_TABLE_SCHEMA = {
    tableName: 'eviction_operations',
    fields: [
      { name: 'id', type: 'text', required: true, primary: true },
      { name: 'ts', type: 'integer', required: true, index: true }, // Unix timestamp ms
      { name: 'op', type: 'text', required: true, index: true }, // INS, ALT, NUL, etc.
      { name: 'target', type: 'json', required: true },
      { name: 'context', type: 'json', required: true },
      { name: 'frame', type: 'json', required: false },
      // Computed/extracted fields for indexing
      { name: 'source_table', type: 'text', computed: 'context.table', index: true },
      { name: 'entity_id', type: 'text', computed: 'target.id', index: true }
    ],
    indexes: [
      { fields: ['entity_id', 'ts'], name: 'idx_entity_timeline' },
      { fields: ['op', 'ts'], name: 'idx_op_timeline' },
      { fields: ['source_table', 'ts'], name: 'idx_table_timeline' }
    ]
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
    createDeleteEvent,
    createConnectionEvent,
    createMergeEvent,

    // Data normalization
    normalizeCase,
    diffCases,

    // Migration/Sync
    convertLegacyActivityToEvents,
    convertSnapshotToEvents,
    fetchAllEvents,
    validateEOState,

    // State reconstruction
    reconstructEntityState,
    reconstructAllStates,
    getAuditTrail,
    getOperatorPattern,
    getFieldHistory,
    getLatestFieldValues,

    // Query utilities
    filterByOperator,
    filterByTimeRange,
    filterByTable,
    getAllEntityIds,
    getCreatedInRange,
    getEntitiesWithFieldChange,

    // Xano API
    fetchEvents,
    pushEvent,
    pushEventsBatch,

    // Format conversion
    toXanoFormat,
    fromXanoFormat,

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
