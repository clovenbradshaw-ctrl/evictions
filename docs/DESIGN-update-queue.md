# Design: Update Queue Table

## Overview

This document proposes a new **append-only update queue table** that tracks which eviction records need refreshing. The queue enables:
- Prioritized, incremental updates of stale data
- Visibility into what's pending refresh
- Decoupled triggering from execution (can batch/throttle API calls to court systems)

---

## Problem Statement

Currently, we have no systematic way to know which records have stale status information. The `_last_updated` field shows when we last recorded data, but:

1. **No staleness detection** - We don't know if a case's court status has changed since our last pull
2. **No update prioritization** - Active cases need more frequent updates than disposed ones
3. **No batch efficiency** - We either refresh everything or nothing

---

## Proposed Schema

### Table: `update_queue`

An **append-only** event log for update requests.

| Field | Type | Description |
|-------|------|-------------|
| `id` | UUID | Primary key |
| `created_at` | TIMESTAMP | When this queue entry was created |
| `entity_id` | TEXT | Docket number (foreign key to eviction_cases) |
| `entity_type` | TEXT | Always 'eviction_case' (future-proofing) |
| `trigger_type` | TEXT | What caused this entry (see triggers below) |
| `priority` | INTEGER | 1=urgent, 2=normal, 3=low |
| `status` | TEXT | 'pending', 'processing', 'completed', 'failed' |
| `processed_at` | TIMESTAMP | When processing completed (null until done) |
| `result` | JSON | Outcome: changes found, errors, etc. |
| `last_known_status` | TEXT | Status when queued (for comparison) |
| `last_updated_at` | TIMESTAMP | Record's _last_updated when queued |

### Indexes
- `(status, priority, created_at)` - For efficient "next to process" queries
- `(entity_id, created_at)` - For checking if entity already queued
- `(trigger_type, created_at)` - For analytics on triggers

---

## Trigger Types

### 1. `STALENESS_AGE` - Time-based staleness

Records that haven't been updated in X days.

```javascript
const STALENESS_THRESHOLDS = {
  'Active': 7,      // Active cases: stale after 7 days
  'Disposed': 30,   // Disposed cases: stale after 30 days
  'Unknown': 14,    // Unknown status: check every 2 weeks
};
```

**Trigger mechanism**: Daily cron job or scheduled function that:
1. Queries all records where `NOW() - _last_updated > threshold`
2. Filters out records already in queue with status='pending'
3. Bulk inserts into update_queue

### 2. `STALENESS_EVENT` - Event-driven staleness signals

Certain events suggest a record needs refresh:

| Signal | Why it triggers | Priority |
|--------|-----------------|----------|
| Judgment date approaching | Likely status change soon | 1 (urgent) |
| Eviction date approaching | Execution may have happened | 1 (urgent) |
| User viewed record | User might want fresh data | 2 (normal) |
| Related case updated | Linked cases may have changed | 2 (normal) |
| CSV export requested | User wants accurate export | 2 (normal) |

### 3. `USER_REQUEST` - Manual refresh request

User explicitly clicks "refresh" on a record or batch.

```javascript
// Example: Add to queue when user clicks refresh
async function requestRecordRefresh(docketNumber) {
  await createQueueEntry({
    entity_id: docketNumber,
    trigger_type: 'USER_REQUEST',
    priority: 1, // User-initiated = urgent
  });
}
```

### 4. `BULK_AUDIT` - Periodic full audit

Weekly/monthly sweep to catch anything missed:
- Compare all records against a "should have been updated by" schedule
- Low priority (3) to not block other updates

### 5. `EXTERNAL_HINT` - External system notification

If court system ever provides webhooks or change feeds:
- Immediate queue entry when notified
- Highest priority

---

## Processing Logic

### Queue Consumer

A background worker (or scheduled function) that:

```javascript
async function processUpdateQueue() {
  // 1. Get next batch (ordered by priority, then created_at)
  const batch = await getNextQueueEntries({
    status: 'pending',
    limit: 50,
    orderBy: ['priority ASC', 'created_at ASC']
  });

  // 2. Mark as processing (prevents double-processing)
  await markAsProcessing(batch.map(e => e.id));

  // 3. Fetch fresh data from court system
  for (const entry of batch) {
    try {
      const freshData = await fetchFromCourtSystem(entry.entity_id);

      // 4. Compare with current state
      const currentRecord = await getRecord(entry.entity_id);
      const changes = detectChanges(currentRecord, freshData);

      // 5. If changes, create ALT event
      if (changes.hasChanges) {
        await createAltEvent(entry.entity_id, changes.delta);
      }

      // 6. Mark as completed
      await markAsCompleted(entry.id, {
        changes_found: changes.hasChanges,
        fields_changed: changes.fieldNames,
      });

    } catch (error) {
      await markAsFailed(entry.id, { error: error.message });
    }
  }
}
```

### Deduplication Strategy

Prevent queue bloat when same record triggered multiple times:

```javascript
async function createQueueEntry(entry) {
  // Check if already pending for this entity
  const existing = await findPendingEntry(entry.entity_id);

  if (existing) {
    // Upgrade priority if new trigger is more urgent
    if (entry.priority < existing.priority) {
      await upgradePriority(existing.id, entry.priority);
    }
    // Don't create duplicate
    return existing;
  }

  return await insertQueueEntry(entry);
}
```

---

## Staleness Detection Algorithm

### Current Staleness Score

Calculate how "stale" each record is:

```javascript
function calculateStalenessScore(record) {
  const daysSinceUpdate = (Date.now() - record._last_updated) / (1000 * 60 * 60 * 24);
  const threshold = STALENESS_THRESHOLDS[record.Status] || 14;

  // Score > 1.0 means stale
  const baseScore = daysSinceUpdate / threshold;

  // Boost score for records with upcoming dates
  let urgencyBoost = 0;

  if (record.eviction_date) {
    const daysUntilEviction = (new Date(record.eviction_date) - Date.now()) / (1000 * 60 * 60 * 24);
    if (daysUntilEviction > 0 && daysUntilEviction < 7) {
      urgencyBoost = 2.0; // Double urgency for upcoming evictions
    }
  }

  return baseScore + urgencyBoost;
}

// Find all stale records
function findStaleRecords(allRecords) {
  return allRecords
    .map(r => ({ record: r, score: calculateStalenessScore(r) }))
    .filter(({ score }) => score >= 1.0)
    .sort((a, b) => b.score - a.score); // Most stale first
}
```

### Dashboard Metrics

Expose staleness visibility:

```javascript
function getStalenessMetrics(records) {
  const stale = findStaleRecords(records);

  return {
    total_records: records.length,
    stale_count: stale.length,
    stale_percentage: (stale.length / records.length * 100).toFixed(1),

    by_status: {
      'Active': stale.filter(s => s.record.Status === 'Active').length,
      'Disposed': stale.filter(s => s.record.Status === 'Disposed').length,
    },

    oldest_update: Math.min(...records.map(r => r._last_updated)),
    average_age_days: calculateAverageAge(records),

    queue_depth: await getQueueDepth(),
    queue_processing_rate: await getProcessingRate(),
  };
}
```

---

## Integration with Existing Architecture

### Fits the Event Sourcing Pattern

The update_queue is **not** part of the core event log (`eviction_operations`). Instead:

```
┌─────────────────┐     triggers     ┌─────────────────┐
│ Staleness       │ ──────────────►  │  update_queue   │
│ Detection       │                  │  (append-only)  │
└─────────────────┘                  └────────┬────────┘
                                              │
                                              │ processes
                                              ▼
                                     ┌─────────────────┐
                                     │ Queue Consumer  │
                                     │ (fetches fresh  │
                                     │  court data)    │
                                     └────────┬────────┘
                                              │
                                              │ creates
                                              ▼
                                     ┌─────────────────┐
                                     │ eviction_       │
                                     │ operations      │
                                     │ (ALT events)    │
                                     └─────────────────┘
```

### Xano Implementation

In Xano, this would be:

1. **New table**: `update_queue` with schema above
2. **Scheduled function**: Runs every hour to detect staleness and enqueue
3. **Background task**: Processes queue entries (rate-limited to avoid court API limits)
4. **API endpoint**: `POST /queue/refresh/{docket_number}` for manual triggers

---

## UI Considerations

### Staleness Indicator

Add visual indicator to records showing staleness:

```javascript
function getStalenessIndicator(record) {
  const score = calculateStalenessScore(record);

  if (score >= 2.0) return { icon: '🔴', label: 'Very stale', class: 'stale-critical' };
  if (score >= 1.0) return { icon: '🟡', label: 'Stale', class: 'stale-warning' };
  if (score >= 0.7) return { icon: '🟢', label: 'Fresh', class: 'stale-ok' };
  return { icon: '✓', label: 'Current', class: 'stale-current' };
}
```

### Queue Status Panel

Dashboard widget showing:
- Records in queue (pending)
- Processing rate (records/hour)
- Failed entries needing attention
- Most stale records not yet queued

---

## Benefits

1. **Visibility**: Know exactly what needs updating and why
2. **Prioritization**: Urgent records (active cases, upcoming evictions) processed first
3. **Efficiency**: Batch processing, rate limiting, deduplication
4. **Audit trail**: Append-only queue shows history of refresh attempts
5. **Resilience**: Failed entries can be retried without losing context
6. **Decoupling**: Triggering and processing are separate concerns

---

## Open Questions

1. **Court API limits**: What's the rate limit for fetching fresh data? This affects queue processing speed.

2. **Notification on changes**: Should users be notified when a queued update finds changes? (e.g., status changed from Active to Disposed)

3. **Historical queue entries**: Keep forever (audit) or prune after X days?

4. **Manual bulk refresh**: Should admins be able to queue all records matching a filter?

5. **Priority override**: Can users mark certain records as "watch closely" for automatic priority boost?

---

## Next Steps

1. [ ] Create `update_queue` table in Xano
2. [ ] Implement staleness scoring function
3. [ ] Add scheduled trigger for time-based staleness
4. [ ] Build queue consumer/processor
5. [ ] Add UI indicators for staleness
6. [ ] Create dashboard metrics panel
