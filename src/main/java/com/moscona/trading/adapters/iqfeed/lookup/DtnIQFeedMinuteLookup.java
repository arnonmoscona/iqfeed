package com.moscona.trading.adapters.iqfeed.lookup;

import com.moscona.exceptions.InvalidArgumentException;
import com.moscona.exceptions.InvalidStateException;
import com.moscona.trading.adapters.iqfeed.DtnIqfeedHistoricalClient;
import com.moscona.trading.adapters.iqfeed.IDtnIQFeedFacade;
import com.moscona.trading.adapters.iqfeed.IQFeedError;
import com.moscona.trading.elements.SymbolChart;
import com.moscona.trading.excptions.MissingSymbolException;
import com.moscona.trading.streaming.TimeSlotBarWithTimeStamp;
import com.moscona.util.ExceptionHelper;
import com.moscona.util.TimeHelper;
import com.moscona.util.async.AsyncFunctionCall;
import com.moscona.util.async.AsyncFunctionFutureResults;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by arnon on 10/2/2014.
 * A draft for a packaged lookup handler
 */
public class DtnIQFeedMinuteLookup extends LookupRequestBase implements ILookupResponseHandler<ArrayList<String[]>> {
    public static final String loggingLabel = "getMinuteBars/minutesBarCall";
    public static final String englishName = "getMinuteBars";

    public DtnIQFeedMinuteLookup(DtnIqfeedHistoricalClient client) {
        super(client);
    }

    @Override
    protected String calendarToString(Calendar calendar) {
        return getClient().toIqFeedTimestamp(calendar);
    }

    @Override
    protected AsyncFunctionCall<List<TimeSlotBarWithTimeStamp>> makeIQFeedCallFunction(String symbol, String startString, String endString, String tag, AsyncFunctionFutureResults<List<TimeSlotBarWithTimeStamp>> pending) {
        return new MinutesBarCall(symbol, startString, endString, tag, getClient().getFacade()).use(pending);
    }

    /**
     * Gets a minute chart for the symbol, possibly with some values missing.
     * Note that the from and to times must both be from the same day, or an exception might be thrown.
     *
     * @param symbol  the symbol for which historic data is requested
     * @param from    the starting time (the beginning of this minute is the beginning of the first minute to be retrieved)
     * @param to      the ending time (the end of this minute is the end of the last minute to be retrieved)
     * @param timeout the maximum time allowed to spend on this operation
     * @param unit    the units for the timeout
     * @param retryLimit the maximum number of allowed retry attempt on errors that justify a retry
     * @return a SymbolChart with the historic data in the time period. Some slots may be null
     * @throws com.moscona.exceptions.InvalidArgumentException
     *
     * @throws com.moscona.exceptions.InvalidStateException
     *
     * @throws java.util.concurrent.TimeoutException
     *
     * @throws com.moscona.trading.excptions.MissingSymbolException, java.util.concurrent.TimeoutException, com.moscona.exceptions.InvalidArgumentException, com.moscona.exceptions.InvalidStateException
     *
     */
    public SymbolChart getMinuteBars(String symbol, Calendar from, Calendar to, int timeout, TimeUnit unit, int retryLimit) throws InvalidArgumentException, InvalidStateException, TimeoutException, MissingSymbolException {
        // when you send a HIT request on the lookup port you get data from the minute starting at the start
        //   * if you request "HIT,INTC,60,20100708 093000,20100708 201000,,,,1,Partial day minutes: INTC"
        //     then the first response will be 2010-07-08 09:31:00
        //   * the last bar will be either the last one available (that had data) or the END of the minute requested
        //     so if the to is "20100708 094000"
        //     then the last point will be "2010-07-08 09:41:00"

        return getBars(symbol, from, to, timeout, unit, retryLimit,
                englishName, englishName+"/MinutesBarCall", this,
                loggingLabel, DtnIqfeedHistoricalClient.ONE_MINUTE_IN_MILLIS);
    }

    /**
     * A method that handles minute bars from HIT requests
     * @param response
     */
    @Override
    public void handleResponse(ArrayList<String[]> response) {
        handleIQFeedBars(response, true, -1, Calendar.MINUTE, "lookup_1min_bars");
    }

    protected class MinutesBarCall extends AsyncFunctionCall<List<TimeSlotBarWithTimeStamp>> {
        private IDtnIQFeedFacade facade = null;
        private String symbol;
        private String tag;
        private String from;
        private String to;

        protected MinutesBarCall(String symbol, String from, String to, String tag, IDtnIQFeedFacade facade) {
            this.symbol = symbol;
            this.from = from;
            this.to = to;
            this.tag = tag;
            this.facade = facade;
        }

        /**
         * Performs the asynchronous call (the "body" of the function). If you need argument (as you probably do) then
         * they should be in the constructor.
         *
         * @throws Exception
         */
        @Override
        protected void asyncCall() throws Exception {
            facade.sendMinuteDataRequest(symbol, from, to, tag);
        }

        /**
         * Computes the argument signature to use for call equivalence. If multiple calls are made withe the same
         * argument signature, they are coalesced into one. If there is already a pending call with the same signature
         * then no additional calls to asyncCall will be made, rather the call will simply share the future result with
         * the other calls.
         *
         * @return the arguments signature to identify equivalent calls
         */
        @Override
        protected String computeArgumentsSignature() {
            return tag;
        }
    }
}
