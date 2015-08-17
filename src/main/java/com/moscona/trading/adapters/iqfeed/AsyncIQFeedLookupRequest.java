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

import com.moscona.trading.adapters.iqfeed.lookup.IQFeedResponseLineAggregator;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Created by Arnon on 5/17/2014.
 */
public interface AsyncIQFeedLookupRequest<ResponseType> {

    /**
     * Executes the request. Multiple calls will issue new independent requests.
     * @return a future for the return value. This future can be used synchronously or asynchronously.
     */
    public CompletableFuture<ResponseType> call();

    /**
     * Returns the retry limit should the call fail (e.g. timeouts)
     * @return the retry limit, with a default of 0 (no retries)
     */
    default public int getParamRetryLimit() { return 0; }

    /**
     * Provide a factory method to create a pending calls store for this type of call.
     * The client will create a single store for all the pending calls of this type. and will reuse it for future calls.
     * Clients are expected to call this method only once.
     * The store maps pending calls, identified by their correlation IDs, to the individual call's response line aggregator.
     * The factory method is passed to the client as part of the invocation of the request, and then the client will use it
     * if it needs it.
     * @return a factory method to make a new pending calls store
     */
    public Supplier<Map<String,IQFeedResponseLineAggregator>> getPendingCallsStoreFactory();
}
