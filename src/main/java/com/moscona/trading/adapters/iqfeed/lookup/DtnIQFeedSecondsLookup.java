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
 * Created by arnon on 10/5/2014.
 * A draft for a packaged lookup handler
 */
public class DtnIQFeedSecondsLookup extends LookupRequestBase implements ILookupResponseHandler<ArrayList<String[]>> {

    protected DtnIQFeedSecondsLookup(DtnIqfeedHistoricalClient client) {
        super(client);
    }

    @Override
    protected AsyncFunctionCall<List<TimeSlotBarWithTimeStamp>> makeIQFeedCallFunction(String symbol, String startString, String endString, String tag, AsyncFunctionFutureResults<List<TimeSlotBarWithTimeStamp>> pending) {
        return null; //FIXME implement this
    }

    @Override
    protected String calendarToString(Calendar calendar) {
        return getClient().toIqFeedTimestamp(calendar);
    }

    /**
     * Gets a second chart for the symbol, possibly with some values missing.
     * Note that the from and to times must both be from the same day, or an exception might be thrown.
     *
     * @param symbol  the symbol for which historic data is requested
     * @param from    the starting time (the beginning of this second is the beginning of the first second to be retrieved)
     * @param to      the ending time (the beginning of this second is the end of the last second to be retrieved)
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
     * @throws com.moscona.exceptions.InvalidArgumentException, InvalidStateException, TimeoutException, MissingSymbolException
     *
     */
    public SymbolChart getSecondBars(String symbol, Calendar from, Calendar to, int timeout, TimeUnit unit, int retryLimit) throws InvalidArgumentException, InvalidStateException, TimeoutException, MissingSymbolException {
//        // The code for this method is almost identical to getMinuteBars() - the main difference is in the handler
//        int requestStarted = TimeHelper.now();
//
//        String startString = toIqFeedTimestamp(from);
//        Calendar requestTo = (Calendar) to.clone();
//        requestTo.add(Calendar.SECOND, -1);
//        String endString = toIqFeedTimestamp(requestTo);
//
//        AsyncFunctionFutureResults<List<TimeSlotBarWithTimeStamp>> pending = getBarsRequestPendingCalls();
//        List<TimeSlotBarWithTimeStamp> answer = null;
//        int remainingAttempts = retryLimit + 1;
//
//        do {
//            remainingAttempts--;
//            String tag = "getSecondBars(" + symbol + ") " + new Date().getTime();
//            try {
//                tagToHandlerIdMap.put(tag, HANDLER_TICKS_TO_SECOND_BARS); // control data routing in the onLookupData method
//                AsyncFunctionCall<List<TimeSlotBarWithTimeStamp>> secondsBarCall = new SecondsBarCall(symbol, startString, endString, tag).use(pending);
//                answer = secondsBarCall.call(timeout, unit);
//                logSymbolTiming(symbol, requestStarted, "getSecondBars/secondsBarCall", null);
//            } catch (IQFeedError e) {
//                handleLookupIQFeedError(e, symbol, requestStarted, "getSecondBars/secondsBarCall", "IQFeedError while trying to get second data for " + symbol + ": " + e);
//            } catch (Exception e) {
//                Throwable iqFeedError = ExceptionHelper.fishOutOf(e, IQFeedError.class, 20);
//                if (iqFeedError != null) {
//                    handleLookupIQFeedError((IQFeedError) iqFeedError, symbol, requestStarted, "getSecondBars/secondsBarCall", "IQFeedError while trying to get second data for " + symbol + ": " + e);
//                } else {
//                    logSymbolTiming(symbol, requestStarted, "getSecondBars/secondsBarCall", e);
//                    throw new InvalidStateException("Exception while trying to get second data for " + symbol + " with timeout " + timeout + " " + unit + ": " + e, e);
//                }
//            } finally {
//                tagToHandlerIdMap.remove(tag); // this tag is unique(ish) and is for one time use
//            }
//        } while (remainingAttempts > 0 && answer == null);
//
//        // verify the return value
//        long expectedCount = 1 + (to.getTimeInMillis() - from.getTimeInMillis()) / 1000;
//        if (expectedCount < answer.size()) {
//            String firstBar = null;
//            String lastBar = null;
//            if (answer.size() > 0) {
//                TimeSlotBarWithTimeStamp first = answer.get(0);
//                if (first != null) {
//                    firstBar = first.toString();
//                }
//
//                TimeSlotBarWithTimeStamp last = answer.get(answer.size() - 1);
//                if (last != null) {
//                    lastBar = last.toString();
//                }
//            }
//            InvalidStateException e = new InvalidStateException("getSecondBars(): Got an answer with the wrong number of bars. Expected " + expectedCount +
//                    " but got " + answer.size() + " bars instead. " +
//                    "the first bar is " + firstBar + " and the last bar is " + lastBar);
//            logSymbolTiming(symbol, requestStarted, "getSecondBars", e);
//            throw e;
//        }
//
//        // Convert the answer into a SymbolChart
//        int oneSecond = 1000;
//        SymbolChart retval = new SymbolChart(symbol,
//                TimeHelper.timeStampRelativeToMidnight(from),
//                TimeHelper.timeStampRelativeToMidnight(to), oneSecond);
//
//        int remaining = retval.capacity();
//        fillChartFromBars(answer, retval, 0);
//
//
//        return retval;
        return null; //FIXME implement this
    }

    @Override
    public void handleResponse(ArrayList<String[]> response) {
        //FIXME implement like in minutes bars, different prefix
    }

    /**
     * The async call for the seconds bar request. the main difference between this and MinutesBarCall is tha the
     * asyncCall here requests ticks from the facade rather than minutes (IQFeed does not do second level granularity)
     */
    protected class SecondsBarCall extends AsyncFunctionCall<List<TimeSlotBarWithTimeStamp>> {
        private IDtnIQFeedFacade facade = null;
        private String symbol;
        private String tag;
        private String from;
        private String to;

        protected SecondsBarCall(String symbol, String from, String to, String tag, IDtnIQFeedFacade facade) {
            this.facade = facade;
            this.symbol = symbol;
            this.from = from;
            this.to = to;
            this.tag = tag;
        }

        /**
         * Performs the asynchronous call (the "body" of the function). If you need argument (as you probably do) then
         * they should be in the constructor.
         *
         * @throws Exception
         */
        @Override
        protected void asyncCall() throws Exception {
            facade.sendTickDataRequest(symbol, from, to, tag);
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
