/*
 * Copyright (c) 2015. Arnon Moscona
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

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

