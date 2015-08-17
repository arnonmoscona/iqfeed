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

package com.moscona.trading.adapters.iqfeed.lookup;

import com.moscona.trading.adapters.iqfeed.AsyncIQFeedLookupRequest;
import com.moscona.trading.adapters.iqfeed.IDtnIQFeedHistoricalClient;
import com.moscona.trading.elements.SymbolChart;
import com.moscona.util.async.AsyncFunctionFutureResults;

import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Created by Arnon on 5/17/2014.
 * An IQFeed client lookup asynchronous request for minute bars
 */
public class MinuteBarsRequest implements AsyncIQFeedLookupRequest<SymbolChart> {
    private final String paramSymbol;
    private final Calendar paramFrom;
    private final Calendar paramTo;
    private final int paramRetryLimit;
    private final IDtnIQFeedHistoricalClient client;

    /**
     * Create, but does not execute the call
     * @param symbol
     * @param from
     * @param to
     * @param retryLimit
     */
    public MinuteBarsRequest(IDtnIQFeedHistoricalClient client,String symbol, Calendar from, Calendar to, int retryLimit) {
        this.client = client;
        paramSymbol = symbol;
        paramFrom = (Calendar)(from.clone());
        paramTo = (Calendar)(to.clone());
        paramTo.set(Calendar.SECOND, 0);
        paramTo.set(Calendar.MILLISECOND, 0);
        paramTo.add(Calendar.MINUTE, -1);

        paramRetryLimit = retryLimit;
    }

    @Override
    public CompletableFuture<SymbolChart> call() {
        return null;  //fixme implement MinuteBarsRequest.call()
        // At some point this will call client.getMinuteBarRequestGenerator(), which will be similar to MinuteBarCall but will just generate the request string for minute bars. This is not weird as this whole class maps directly into minute bars calls, but for instance, the original seconds bar request was mapping to tick requests and was then aggregating those to seconds (I think IQFeed now supports second bars directly)
        // At some point this will call client.lookupCall(iQFeedRequestString, responseLineAggregator)
    }

    @Override
    public Supplier<Map<String, IQFeedResponseLineAggregator>> getPendingCallsStoreFactory() {
        return null;  //fixme implement MinuteBarsRequest.getPendingCallsStoreFactory()
    }

    @Override
    public int getParamRetryLimit() {
        return paramRetryLimit;
    }
}
