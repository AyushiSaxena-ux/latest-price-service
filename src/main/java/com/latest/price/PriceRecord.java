package com.latest.price;

import java.time.Instant;

public class PriceRecord {

    private final String id;
    private final Instant asOf;
    //Created it as type of Object as it needs to be flexible
    private final Object payload;

    public PriceRecord(String id, Instant asOf, Object payload) {
        this.id = id;
        this.asOf = asOf;
        this.payload = payload;
    }

    public String getId() {
        return id;
    }

    public Instant getAsOf() {
        return asOf;
    }

    public Object getPayload() {
        return payload;
    }
}
