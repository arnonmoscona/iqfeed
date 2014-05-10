package com.moscona.trading.adapters.iqfeed;

/**
 * Created: Jul 23, 2010 7:09:52 AM
 * By: Arnon Moscona
 * A class that that represents an abstract message queue item with insertion timestamps
 */
public class MessageQueueItem<T> {
    private T payload;
    private long insertionTimestamp;
    private long pickupTimestamp;

    public MessageQueueItem(T payload) {
        this.payload = payload;
        insertionTimestamp = System.currentTimeMillis();
        pickupTimestamp = -1;
    }

    public T getPayload() {
        if (pickupTimestamp < 0) {
            pickupTimestamp = System.currentTimeMillis();
        }
        return payload;
    }

    /**
     * Gets the time in millis between creation and first pickup
     * @return
     */
    public long getLatency() {
        return pickupTimestamp - insertionTimestamp;
    }
}

