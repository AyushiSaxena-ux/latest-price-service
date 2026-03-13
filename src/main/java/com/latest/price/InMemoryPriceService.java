package com.latest.price;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
/*
 We use a copy-on-write snapshot design.

 Consumers always read from an immutable snapshot map.
 When a batch completes we create a new map, apply updates,
 and atomically swap the reference.

 This guarantees:
  - atomic visibility of batch updates
  - lock-free reads
  - consistent view for consumers
*/

public class InMemoryPriceService implements PriceService {

    private final AtomicReference<Map<String, PriceRecord>> snapshot =
            new AtomicReference<>(new HashMap<>());

    private final ConcurrentMap<String, Batch> batches =
            new ConcurrentHashMap<>();

    @Override
    public String startBatch() {

        String batchId = UUID.randomUUID().toString();
        batches.put(batchId, new Batch());

        return batchId;
    }

    @Override
    public void uploadChunk(String batchId, List<PriceRecord> records) {

        Batch batch = batches.get(batchId);

        if (batch == null) {
            throw new IllegalStateException("Invalid batch " + batchId);
        }

        for (PriceRecord r : records) {
            batch.addRecord(r);
        }
    }

    @Override
    public void completeBatch(String batchId) {

        Batch batch = batches.remove(batchId);

        if (batch == null) {
            throw new IllegalStateException("Invalid batch " + batchId);
        }

        // current snapshot
        Map<String, PriceRecord> current = snapshot.get();

        // copy
        Map<String, PriceRecord> newSnapshot =
                new HashMap<>(current);

        for (PriceRecord record : batch.getRecords().values()) {

            PriceRecord existing = newSnapshot.get(record.getId());

            if (existing == null ||
                    record.getAsOf().isAfter(existing.getAsOf())) {

                newSnapshot.put(record.getId(), record);
            }
        }

        // atomic swap
        snapshot.set(Collections.unmodifiableMap(newSnapshot));
    }

    @Override
    public void cancelBatch(String batchId) {

        batches.remove(batchId);
    }

    @Override
    public Map<String, PriceRecord> getLastPrices(List<String> ids) {

        Map<String, PriceRecord> result = new HashMap<>();

        Map<String, PriceRecord> current = snapshot.get();

        for (String id : ids) {

            PriceRecord record = current.get(id);

            if (record != null) {
                result.put(id, record);
            }
        }

        return result;
    }
}
