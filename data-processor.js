/**
 * Web Worker for offloading data processing (dedup, normalization)
 * This keeps the main thread responsive during heavy data operations
 */

// Check if a value is meaningful (not null, undefined, or empty string)
function hasValue(val) {
    return val !== null && val !== undefined && val !== '';
}

// Merge two records, preferring non-null/non-empty values
function mergeRecordFields(existing, newer) {
    const merged = { ...newer };
    for (const key of Object.keys(existing)) {
        if (!hasValue(merged[key]) && hasValue(existing[key])) {
            merged[key] = existing[key];
        }
    }
    return merged;
}

// Reduce activity stream to get current state per docket (with case_data wrapper)
function reduceActivityStream(activityRecords) {
    const byDocket = new Map();

    for (const record of activityRecords) {
        const docket = record.docket_number;
        if (!docket) continue;

        const existing = byDocket.get(docket);
        if (!existing) {
            byDocket.set(docket, record);
        } else if (record.logged_at > existing.logged_at) {
            byDocket.set(docket, mergeRecordFields(existing, record));
        } else {
            byDocket.set(docket, mergeRecordFields(record, existing));
        }
    }

    return Array.from(byDocket.values()).map(record => {
        const caseData = record.case_data || {};
        return {
            _logged_at: record.logged_at,
            _action: record.action,
            _activity_id: record.id,
            Docket_Number: record.docket_number || caseData.Docket_Number || caseData.docket_number,
            File_Date: caseData.File_Date || caseData.file_date,
            Status: caseData.Status || caseData.status,
            Office: caseData.Office || caseData.office,
            Description: caseData.Description || caseData.description,
            Plaintiff_Petitioner: caseData.Plaintiff_Petitioner || caseData.plaintiff_petitioner || caseData.plaintiff,
            Defendant_Respondent: caseData.Defendant_Respondent || caseData.defendant_respondent || caseData.defendant,
            Attorney: caseData.Attorney || caseData.attorney || caseData.plaintiff_attorney,
            Defendant_Attorney: caseData.defendant_attorney || caseData.Defendant_Attorney || caseData.defense_attorney,
            Address_1: caseData.Address_1 || caseData.address_1 || caseData.address,
            Address_2: caseData.Address_2 || caseData.address_2,
            city_state_zip: caseData.city_state_zip || caseData.City_State_Zip,
            address_formatted: caseData.address_formatted || caseData.formatted_address,
            formatted_address: caseData.formatted_address || caseData.address_formatted,
            latitude: caseData.latitude || caseData.lat,
            longitude: caseData.longitude || caseData.lng || caseData.lon,
            council_district: caseData.council_district || caseData.Council_District,
            council_district_number: caseData.council_district_number || caseData.Council_District_Number,
            council_member: caseData.council_member || caseData.Council_Member,
            individual_ll: caseData.individual_ll || caseData.Individual_LL
        };
    });
}

// Reduce activity stream when data fields are at top level (no case_data wrapper)
function reduceActivityStreamFlat(activityRecords) {
    const byDocket = new Map();

    for (const record of activityRecords) {
        const docket = record.docket_number;
        if (!docket) continue;

        const existing = byDocket.get(docket);
        if (!existing) {
            byDocket.set(docket, record);
        } else if (record.logged_at > existing.logged_at) {
            byDocket.set(docket, mergeRecordFields(existing, record));
        } else {
            byDocket.set(docket, mergeRecordFields(record, existing));
        }
    }

    return Array.from(byDocket.values()).map(record => ({
        _logged_at: record.logged_at,
        _action: record.action,
        _activity_id: record.id,
        Docket_Number: record.docket_number || record.Docket_Number,
        File_Date: record.File_Date || record.file_date,
        Status: record.Status || record.status,
        Office: record.Office || record.office,
        Description: record.Description || record.description,
        Plaintiff_Petitioner: record.Plaintiff_Petitioner || record.plaintiff_petitioner || record.plaintiff,
        Defendant_Respondent: record.Defendant_Respondent || record.defendant_respondent || record.defendant,
        Attorney: record.Attorney || record.attorney || record.plaintiff_attorney,
        Defendant_Attorney: record.defendant_attorney || record.Defendant_Attorney || record.defense_attorney,
        Address_1: record.Address_1 || record.address_1 || record.address,
        Address_2: record.Address_2 || record.address_2,
        city_state_zip: record.city_state_zip || record.City_State_Zip,
        address_formatted: record.address_formatted || record.formatted_address,
        formatted_address: record.formatted_address || record.address_formatted,
        latitude: record.latitude || record.lat,
        longitude: record.longitude || record.lng || record.lon,
        council_district: record.council_district || record.Council_District,
        council_district_number: record.council_district_number || record.Council_District_Number,
        council_member: record.council_member || record.Council_Member,
        individual_ll: record.individual_ll || record.Individual_LL
    }));
}

// Normalize a record to a consistent format
function normalizeRecord(record) {
    // Extract district number from various possible field names/formats
    let district = null;
    const districtSources = [
        record.council_district_number,
        record.Council_District_Number,
        record.council_district,
        record.Council_District,
        record.District,
        record.district
    ];

    for (const source of districtSources) {
        if (source !== null && source !== undefined && source !== '') {
            if (typeof source === 'number') {
                district = source;
                break;
            }
            if (typeof source === 'string') {
                const match = source.match(/(\d+)/);
                if (match) {
                    district = parseInt(match[1], 10);
                    break;
                }
            }
        }
    }

    // Normalize the record
    return {
        Case_Number: record.Docket_Number || record.docket_number || record.Case_Number || record.case_number || '',
        District: district,
        File_Date: record.File_Date || record.file_date || '',
        Status: record.Status || record.status || '',
        Office: record.Office || record.office || '',
        Description: record.Description || record.description || '',
        Plaintiff_Petitioner: record.Plaintiff_Petitioner || record.plaintiff_petitioner || record.Plaintiff || record.plaintiff || '',
        Defendant_Respondent: record.Defendant_Respondent || record.defendant_respondent || record.Defendant || record.defendant || '',
        Attorney: record.Attorney || record.attorney || record.plaintiff_attorney || '',
        Defendant_Attorney: record.Defendant_Attorney || record.defendant_attorney || record.defense_attorney || '',
        Address: record.address_formatted || record.formatted_address || record.Address_1 || record.address_1 || record.address || '',
        CouncilMember: record.council_member || record.Council_Member || '',
        latitude: record.latitude || record.lat || null,
        longitude: record.longitude || record.lng || record.lon || null,
        individual_ll: record.individual_ll || record.Individual_LL || false,
        _logged_at: record._logged_at || record.logged_at || null,
        _action: record._action || record.action || null
    };
}

// Deduplicate records by docket number, keeping the one with most complete data
function dedupeByDocketNumber(records) {
    const byDocket = new Map();

    for (const record of records) {
        const key = (record.Case_Number || '').trim();
        if (!key) continue;

        const existing = byDocket.get(key);
        if (!existing) {
            byDocket.set(key, record);
        } else {
            // Merge, preferring newer/more complete data
            byDocket.set(key, mergeRecordFields(existing, record));
        }
    }

    return Array.from(byDocket.values());
}

// Compress eviction data to essential fields
function compressEvictionData(records) {
    return records.map(r => ({
        id: r.Case_Number || r.case_number || r.Docket_Number || '',
        d: r.District || null,
        fd: r.File_Date || r.file_date || '',
        p: r.Plaintiff_Petitioner || r.plaintiff_petitioner || r.Plaintiff || '',
        def: r.Defendant_Respondent || r.defendant_respondent || r.Defendant || '',
        s: r.Status || r.status || '',
        a: r.Attorney || r.attorney || '',
        da: r.Defendant_Attorney || r.defendant_attorney || '',
        addr: r.Address || r.address_formatted || '',
        cm: r.CouncilMember || r.council_member || '',
        lat: r.latitude || null,
        lng: r.longitude || null,
        ill: r.individual_ll || false,
        la: r._logged_at || null
    }));
}

// Decompress eviction data back to expected format
function decompressEvictionData(compressed) {
    return compressed.map(r => ({
        Case_Number: r.id,
        District: r.d,
        File_Date: r.fd,
        Plaintiff_Petitioner: r.p,
        Defendant_Respondent: r.def,
        Status: r.s,
        Attorney: r.a,
        Defendant_Attorney: r.da,
        Address: r.addr,
        CouncilMember: r.cm,
        latitude: r.lat,
        longitude: r.lng,
        individual_ll: r.ill,
        _logged_at: r.la
    }));
}

// Main message handler
self.onmessage = function(e) {
    const { type, data, chunkIndex, totalChunks } = e.data;

    switch (type) {
        case 'PROCESS_CHUNK': {
            // Process a chunk of raw API data
            const startTime = performance.now();

            // Detect format and reduce activity stream
            const isActivityStream = data.length > 0 && data[0].logged_at !== undefined && data[0].case_data !== undefined;
            const hasActivityMetadata = data.length > 0 && data[0].logged_at !== undefined;

            let flattenedData;
            if (isActivityStream) {
                flattenedData = reduceActivityStream(data);
            } else if (hasActivityMetadata) {
                flattenedData = reduceActivityStreamFlat(data);
            } else {
                flattenedData = data;
            }

            // Normalize all records
            const normalizedData = flattenedData.map(normalizeRecord);

            // Track newest logged_at
            let newestLoggedAt = null;
            for (const record of data) {
                if (record.logged_at && (!newestLoggedAt || record.logged_at > newestLoggedAt)) {
                    newestLoggedAt = record.logged_at;
                }
            }

            const processingTime = performance.now() - startTime;

            self.postMessage({
                type: 'CHUNK_PROCESSED',
                data: normalizedData,
                newestLoggedAt,
                chunkIndex,
                totalChunks,
                stats: {
                    inputCount: data.length,
                    outputCount: normalizedData.length,
                    processingTimeMs: processingTime
                }
            });
            break;
        }

        case 'DEDUPE': {
            // Deduplicate accumulated records
            const startTime = performance.now();
            const deduped = dedupeByDocketNumber(data);
            const processingTime = performance.now() - startTime;

            self.postMessage({
                type: 'DEDUPE_COMPLETE',
                data: deduped,
                stats: {
                    inputCount: data.length,
                    outputCount: deduped.length,
                    duplicatesRemoved: data.length - deduped.length,
                    processingTimeMs: processingTime
                }
            });
            break;
        }

        case 'COMPRESS': {
            // Compress data for storage
            const compressed = compressEvictionData(data);
            self.postMessage({
                type: 'COMPRESS_COMPLETE',
                data: compressed
            });
            break;
        }

        case 'DECOMPRESS': {
            // Decompress stored data
            const decompressed = decompressEvictionData(data);
            self.postMessage({
                type: 'DECOMPRESS_COMPLETE',
                data: decompressed
            });
            break;
        }

        case 'MERGE': {
            // Merge new data with existing data
            const { existingData, newData } = data;
            const startTime = performance.now();

            const dataMap = new Map();

            // Add existing data first
            for (const record of existingData) {
                const key = (record.Case_Number || '').trim();
                if (key) dataMap.set(key, record);
            }

            // Add/update with new data
            let updatedCount = 0;
            let addedCount = 0;
            for (const record of newData) {
                const key = (record.Case_Number || '').trim();
                if (key) {
                    if (dataMap.has(key)) {
                        dataMap.set(key, mergeRecordFields(dataMap.get(key), record));
                        updatedCount++;
                    } else {
                        dataMap.set(key, record);
                        addedCount++;
                    }
                }
            }

            const merged = Array.from(dataMap.values());
            const processingTime = performance.now() - startTime;

            self.postMessage({
                type: 'MERGE_COMPLETE',
                data: merged,
                stats: {
                    existingCount: existingData.length,
                    newCount: newData.length,
                    addedCount,
                    updatedCount,
                    totalCount: merged.length,
                    processingTimeMs: processingTime
                }
            });
            break;
        }

        default:
            console.warn('Unknown message type:', type);
    }
};
