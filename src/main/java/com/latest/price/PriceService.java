package com.latest.price;

import java.util.List;
import java.util.Map;

public interface PriceService {

    String startBatch();

    void uploadChunk(String batchId, List<PriceRecord> records);

    void completeBatch(String batchId);

    void cancelBatch(String batchId);

    Map<String, PriceRecord> getLastPrices(List<String> ids);
}