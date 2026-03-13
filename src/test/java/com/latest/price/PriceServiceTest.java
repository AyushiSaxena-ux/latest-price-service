package com.latest.price;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

/*
 Tests verify:
 1. Batch commit visibility
 2. Cancelled batches are discarded
 3. Latest timestamp wins
 4. Consumers never see partial batches
*/
class PriceServiceTest {

    private final PriceService service = new InMemoryPriceService();

    @Test
    void testBasicBatchCommit() {

        String batchId = service.startBatch();

        PriceRecord r1 = new PriceRecord("AAPL", Instant.now(), 150);
        PriceRecord r2 = new PriceRecord("GOOG", Instant.now(), 200);

        service.uploadChunk(batchId, Arrays.asList(r1, r2));
        service.completeBatch(batchId);

        Map<String, PriceRecord> result =
                service.getLastPrices(Arrays.asList("AAPL", "GOOG"));

        assertEquals(150, result.get("AAPL").getPayload());
        assertEquals(200, result.get("GOOG").getPayload());
    }

    @Test
    void testCancelBatch() {

        String batchId = service.startBatch();

        PriceRecord r1 = new PriceRecord("MSFT", Instant.now(), 300);

        service.uploadChunk(batchId, Arrays.asList(r1));
        service.cancelBatch(batchId);

        Map<String, PriceRecord> result =
                service.getLastPrices(Arrays.asList("MSFT"));

        assertTrue(result.isEmpty());
    }

    @Test
    void testLatestAsOfWins() {

        String batchId = service.startBatch();

        PriceRecord older =
                new PriceRecord("AAPL", Instant.parse("2026-01-01T10:00:00Z"), 100);

        PriceRecord newer =
                new PriceRecord("AAPL", Instant.parse("2026-01-01T11:00:00Z"), 200);

        service.uploadChunk(batchId, Arrays.asList(older, newer));
        service.completeBatch(batchId);

        Map<String, PriceRecord> result =
                service.getLastPrices(Arrays.asList("AAPL"));

        assertEquals(200, result.get("AAPL").getPayload());
    }

    @Test
    void consumerShouldNotSeePartialBatchWithThreads() throws Exception {

        PriceService service = new InMemoryPriceService();

        // Initial committed price
        String initialBatch = service.startBatch();
        service.uploadChunk(
                initialBatch,
                Arrays.asList(
                        new PriceRecord("AAPL",
                                Instant.parse("2026-03-12T10:00:00Z"),
                                100)
                )
        );
        service.completeBatch(initialBatch);

        ExecutorService executor = Executors.newFixedThreadPool(2);

        CountDownLatch uploadDone = new CountDownLatch(1);
        CountDownLatch allowCommit = new CountDownLatch(1);

        // Producer thread
        executor.submit(() -> {

            String batchId = service.startBatch();

            service.uploadChunk(
                    batchId,
                    Arrays.asList(
                            new PriceRecord(
                                    "AAPL",
                                    Instant.parse("2026-03-12T11:00:00Z"),
                                    200)
                    )
            );

            // Signal upload finished but batch not committed
            uploadDone.countDown();

            try {
                // wait until consumer reads
                allowCommit.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            service.completeBatch(batchId);
        });

        // Consumer thread
        Future<Integer> consumerResult = executor.submit(() -> {

            // Wait until upload finished
            uploadDone.await();

            Map<String, PriceRecord> prices =
                    service.getLastPrices(Arrays.asList("AAPL"));

            // Consumer should still see old value
            int price = (int) prices.get("AAPL").getPayload();

            // allow batch commit
            allowCommit.countDown();

            return price;
        });

        int observedPrice = consumerResult.get();

        assertEquals(100, observedPrice);

        executor.shutdown();
    }
}
