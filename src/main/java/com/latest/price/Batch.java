package com.latest.price;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
/*

We keep only the newest record per instrument inside the batch.
This reduces memory.

 */
class Batch {

    private final ConcurrentMap<String, PriceRecord> records = new ConcurrentHashMap<>();

    public void addRecord(PriceRecord record) {
        // Add newest record into records map based on latest AsOf
        records.merge(
                record.getId(),
                record,
                (existing, incoming) ->
                        incoming.getAsOf().isAfter(existing.getAsOf()) ? incoming : existing
        );
    }

    public ConcurrentMap<String, PriceRecord> getRecords() {
        return records;
    }
}
