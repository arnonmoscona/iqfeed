package com.moscona.trading.adapters.iqfeed.lookup;

import com.moscona.exceptions.InvalidArgumentException;
import com.moscona.exceptions.InvalidStateException;
import com.moscona.trading.adapters.iqfeed.DtnIqfeedHistoricalClient;
import com.moscona.trading.adapters.iqfeed.IQFeedError;
import com.moscona.trading.elements.SymbolChart;
import com.moscona.trading.excptions.MissingSymbolException;
import com.moscona.trading.streaming.TimeSlotBarWithTimeStamp;
import com.moscona.util.ExceptionHelper;
import com.moscona.util.TimeHelper;
import com.moscona.util.async.AsyncFunctionCall;
import com.moscona.util.async.AsyncFunctionFutureResults;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by arnon on 10/2/2014.
 * Base for lookup request and handling classes
 */
public abstract class LookupRequestBase {

    /**
     * The client for which this call is working
     */
    private DtnIqfeedHistoricalClient client;
    private String tag;

    protected LookupRequestBase(DtnIqfeedHistoricalClient client) {
        this.client = client;
    }

    protected DtnIqfeedHistoricalClient getClient() {
        return client;
    }

    protected String makeTag(String methodName, String symbol) {
        tag = methodName+ "(" + symbol + ") " + new Date().getTime();
        return tag;
    }

    public String getTag() {
        return tag;
    }

    /**
     * A base method helpng sub-class implementation of the main service method.
     * Gets a [time unit] chart for the symbol, possibly with some values missing.
     * Note that the from and to times must both be from the same day, or an exception might be thrown.
     *
     * @param symbol  the symbol for which historic data is requested
     * @param from    the starting time (the beginning of this minute is the beginning of the first minute to be retrieved)
     * @param to      the ending time (the end of this minute is the end of the last minute to be retrieved)
     * @param timeout the maximum time allowed to spend on this operation
     * @param unit    the units for the timeout
     * @param retryLimit   the maximum number of allowed retry attempt on errors that justify a retry
     * @param englishName  the name of the "method"
     * @param tagPrefix    the prefix for the tag to use for the IQFeed call
     * @param caller       the concrete caller
     * @param loggingLabel a text label used for identifying the context in logging
     * @param granularityInMillis the number of millis in the granularity time unit
     *
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
    protected SymbolChart getBars(String symbol, Calendar from, Calendar to, int timeout, TimeUnit unit, int retryLimit, String englishName, String tagPrefix, ILookupResponseHandler<ArrayList<String[]>> caller, String loggingLabel, int granularityInMillis) throws InvalidArgumentException, InvalidStateException, TimeoutException, MissingSymbolException {
        // when you send a HIT request on the lookup port you get data from the minute starting at the start
        //   * if you request "HIT,INTC,60,20100708 093000,20100708 201000,,,,1,Partial day minutes: INTC"
        //     then the first response will be 2010-07-08 09:31:00
        //   * the last bar will be either the last one available (that had data) or the END of the minute requested
        //     so if the to is "20100708 094000"
        //     then the last point will be "2010-07-08 09:41:00"

        // calculate boundary times
        int requestStarted = TimeHelper.now();

        Calendar start = (Calendar) from.clone();
        Calendar end = (Calendar) to.clone();
        end.set(Calendar.SECOND, 0);
        end.set(Calendar.MILLISECOND, 0);
        end.add(Calendar.MINUTE, -1);

        String startString = calendarToString(start);
        String endString = calendarToString(end);

        AsyncFunctionFutureResults<List<TimeSlotBarWithTimeStamp>> pending = getClient().getBarsRequestPendingCalls();
        int remainingAttempts = retryLimit + 1;
        List<TimeSlotBarWithTimeStamp> answer = null;

        do {
            remainingAttempts--;
            String tag = makeTag(tagPrefix, symbol);
            getClient().registerResponseHandler(tag, caller); // control data routing in the onLookupData method

            AsyncFunctionCall<List<TimeSlotBarWithTimeStamp>> requestCall = makeIQFeedCallFunction(symbol,startString, endString, tag, pending);
            answer = null;

            try {
                answer = requestCall.call(timeout, unit);
                getClient().logSymbolTiming(symbol, requestStarted, loggingLabel, null);
            } catch (IQFeedError e) {
                getClient().handleLookupIQFeedError(e, symbol, requestStarted, loggingLabel, "IQFeedError while trying to get historical data for " + symbol + ": " + e);
            } catch (Exception e) {
                Throwable iqFeedError = ExceptionHelper.fishOutOf(e, IQFeedError.class, 20);
                if (iqFeedError != null) {
                    getClient().handleLookupIQFeedError((IQFeedError) iqFeedError, symbol, requestStarted, loggingLabel, "IQFeedError while trying to get historical data for " + symbol + ": " + e);
                } else {
                    getClient().logSymbolTiming(symbol, requestStarted, loggingLabel, e);
                    throw new InvalidStateException("Exception while trying to get historical data for " + symbol + " with timeout " + timeout + " " + unit + ": " + e, e);
                }
            } finally {
                // Not needed any more: tagToHandlerIdMap.remove(tag); // this tag is unique(ish) and is for one time use
            }
        } while (remainingAttempts > 0 && answer == null);

        // verify the return value
        long expectedCount = (to.getTimeInMillis() - from.getTimeInMillis()) / granularityInMillis;
        if (expectedCount < answer.size()) {
            String firstBar = null;
            String lastBar = null;
            if (answer.size() > 0) {
                TimeSlotBarWithTimeStamp first = answer.get(0);
                if (first != null) {
                    firstBar = first.toString();
                }

                TimeSlotBarWithTimeStamp last = answer.get(answer.size() - 1);
                if (last != null) {
                    lastBar = last.toString();
                }
            }
            InvalidStateException e = new InvalidStateException("getMinuteBars(): Got an answer with the wrong number of bars. Expected " + expectedCount +
                    " but got " + answer.size() + " bars instead. " +
                    "the first bar is " + firstBar + " and the last bar is " + lastBar);
            getClient().logSymbolTiming(symbol, requestStarted, englishName, e);
            throw e;
        }

        // Convert the answer into a SymbolChart
        SymbolChart retval = new SymbolChart(symbol,
                TimeHelper.timeStampRelativeToMidnight(from),
                TimeHelper.timeStampRelativeToMidnight(to), granularityInMillis);

        // we may not have gotten an answer for every minute
        fillChartFromBars(answer, retval, 0);

        return retval;
    }

    /**
     * Make the appropriate conversion from Calendar to an IQFeed time string for the specific call
     * @param calendar the Calendar to convert
     * @return
     */
    protected abstract String calendarToString(Calendar calendar);

    /**
     * Makes the IQFeed call function. The easiest is something like this:
     * new MinutesBarCall(symbol, startString, endString, tag, getClient().getFacade()).use(pending)
     * Where MinutesBarCall extends AsyncFunctionCall<List<TimeSlotBarWithTimeStamp>>
     * @param symbol
     * @param startString
     * @param endString
     * @param tag
     * @param pending
     * @return
     */
    protected abstract AsyncFunctionCall<List<TimeSlotBarWithTimeStamp>> makeIQFeedCallFunction(String symbol, String startString, String endString, String tag, AsyncFunctionFutureResults<List<TimeSlotBarWithTimeStamp>> pending);

    protected  void handleIQFeedBars(ArrayList<String[]> list, boolean useSecondVolumeNumber, int unitIncrement, int unit, String prefix) {
        // FIXME This should be factored out as a part of a pluggable add-on
        if (list.size() == 0) {
            return; // nothing to do and don't know how to tell any pending call
        }
        String[] first = list.get(0);
        String tag = first[0];

        // sending command: HIT,INTC,60,20100708 093000,20100708 094000,,,,1,Partial day minutes: INTC
        // Response:
        // Partial day minutes: INTC,2010-07-08 09:31:00,20.3600,20.2200,20.3400,20.2805,1266398,502507,
        // tag,minute end,high,low,open,close,volume,minute volume,
        //
        // sending command: HDT,CLB,20090731,20100731,,1,oneYear CLB
        // Response:
        // oneYear CLB,2009-08-03 00:00:00,44.8650,43.5300,43.6400,44.8650,395520,0,
        // tag,date (following),high,low,open,close,volume,open interest,
        //
        // No data response:
        // Partial day minutes: INTX,E,!NO_DATA!,
        // Invalid symbol response:
        // Partial day minutes: INTCX,E,Invalid symbol.,


        AsyncFunctionFutureResults<List<TimeSlotBarWithTimeStamp>> pending = client.getBarsRequestPendingCalls();

        // Check for error message
        if (first[1].equals("E")) {
            String literalError = first[2];
            if (literalError.equals(DtnIqfeedHistoricalClient.IQFEED_NO_DATA_RESPONSE)) {
                // make an empty response
                pending.returnToCallers(tag, new ArrayList<TimeSlotBarWithTimeStamp>(), client.getConfig().getLookupStatsService(), prefix);
                return; // and there's nothing else to do here
            }

            boolean canRetry = client.canRetry(literalError);
            pending.throwException(tag, new IQFeedError(literalError, "Error received in response to bar request tagged: '" +
                    tag + "'  literal error: '" + literalError + "'", canRetry));
            return;
        }

        String[] currentLine = null;

        try {
            ArrayList<TimeSlotBarWithTimeStamp> retval = new ArrayList<TimeSlotBarWithTimeStamp>();  // TimeSlotBar, actually

            for (String[] line : list) {
                currentLine = line;

                if (line[1].equals("E")) {
                    pending.throwException(tag, new Exception("Error received in response to bar request tagged: '" +
                            tag + "'  literal error: '" + line[2] + "'"));
                    return;
                }

                int field = 1;
                String timestampString = line[field++];
                Calendar timestamp = TimeHelper.parse(timestampString); // FIXME This should be extracted from TimeHelper
                timestamp.add(unit, unitIncrement); // IQFeed gives back as timestamp the closing time!
                float high = Float.parseFloat(line[field++].trim());
                float low = Float.parseFloat(line[field++].trim());
                float open = Float.parseFloat(line[field++].trim());
                float close = Float.parseFloat(line[field++].trim());
                String volume1String = line[field++].trim();
                String volume2String = line[field].trim();

                TimeSlotBarWithTimeStamp bar = new TimeSlotBarWithTimeStamp();
                bar.set(toCents(open), toCents(close), toCents(high), toCents(low), Integer.parseInt(useSecondVolumeNumber ? volume2String : volume1String));
                bar.setTimestamp(timestamp);

                bar.close();
                retval.add(bar);
            }

            pending.returnToCallers(tag, retval, client.getConfig().getLookupStatsService(), prefix);
        } catch (Exception e) {
            pending.throwException(tag, new Exception("handleIQFeedBars(): Error while processing line: " + StringUtils.join(currentLine, ',') + " :" + e, e), client.getConfig().getLookupStatsService(), prefix);
        }
    }

    protected int toCents(float price) {
        return Math.round(price * 100.0f);
    }

    protected void fillChartFromBars(List<TimeSlotBarWithTimeStamp> answer, SymbolChart retval, int indexOffset) throws InvalidArgumentException {
        // now, the trick is that you may not get bars for every period, so you must be careful about which bars are for which period
        int currentBarIndex = 0;
        int currentTimestamp = retval.getStartTimeStamp();
        for (int i = 0; i < retval.capacity(); i++, currentTimestamp += retval.getGranularityMillis()) {
            if (answer.size() > currentBarIndex) {
                TimeSlotBarWithTimeStamp bar = answer.get(currentBarIndex);
                int currentBarTimestamp = TimeHelper.convertToInternalTs(bar.getTimestamp(), true) + indexOffset * retval.getGranularityMillis();
                if (currentTimestamp / retval.getGranularityMillis() == currentBarTimestamp / retval.getGranularityMillis()) {
                    // both are the same period
                    retval.setBar(bar, i); // put it in the result
                    currentBarIndex++; // advance to the next candidate
                }
            }
        }
    }
}
