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
