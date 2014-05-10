package com.moscona.trading.adapters.iqfeed;

import com.moscona.util.async.AsyncFunctionFutureResults;
import com.moscona.exceptions.InvalidArgumentException;
import com.moscona.exceptions.InvalidStateException;
import com.moscona.trading.elements.Bar;
import com.moscona.trading.streaming.TimeSlotBarWithTimeStamp;
import com.moscona.util.TimeHelper;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Arnon on 5/8/2014.
 * Derived from code from another project.
 */
public class DtnIqfeedHistoricalClient implements IDtnIQFeedClient {
    /**
     * **************************************************************************************************************
     * IMPORTANT: A recipe for writing a sync lookup requests (like getMinuteBars()):
     * 1. You need a variable for storing the pending calls. See example in getDailyDataPendingCalls()
     * 2. You need a class to represent the async call. See example in DtnIqfeedHistoricalClient.MinutesBarCall
     * 3. You need to calculate a tag or other identifier for your particular request
     * 4. The confusing part is returning data. Here it is not done directly. The requesting class sends the
     *    request into the great blue yonder. How does the data come back?
     *    In most cases here (not all) the path of the returning data goes through onLookupData()
     * 5. In your call you need to make sure that you know which function will handle the response data.
     *    In the lookup cases here this is done via associating tags with handlers, such as the HANDLER_MINUTE_BARS
     * 6. In those cases the handling routed in removeFromPendingAndHandle() to the correct handler method, which
     *    you need to write, if no other existing handling method already matches the data format you expect.
     * 7. Once the handler gets your pending call using the tag you provided earlier, it will send you the result
     *    via the pending request, at which point your request returns (or throws an exception if the handler
     *    decided that it needs to send you and exception) and your back in your normal call/response synchronous
     *    world...
     *
     * Future implementation note: could simplify the whole thing by using something like Apache Camel...
     * ***************************************************************************************************************
     */

    public static final int HEARTBEAT_SLEEP_MILLIS = 100;
    public static final int ONE_MINUTE_IN_MILLIS = 60000;
    public static final int MILLIS_PER_MINUTE = ONE_MINUTE_IN_MILLIS;
    public static final String IQFEED_ERROR_ALERT_MESSAGE_TYPE = "IQFeed Error";
    public static final String IQFEED_LOOKUP_ERROR_ALERT_MESSAGE_TYPE = "IQFeed Error (lookup)";
    public static final String IQFEED_COMPATIBLE_VERSION = "4.7.2.0";
    public static final int DEFAULT_HICCUP_INTERVAL_MILLIS = 500;
    public static final String IQFEED_NO_DATA_RESPONSE = "!NO_DATA!";


    public static final String TAG_PREFIX_31_DAY = "31 day ";
    public static final int HANDLER_31_DAY_BARS = 0; // handler ID for the data handler for 31 day bars request
    public static final int HANDLER_MINUTE_BARS = 1; // handler ID for the data handler for minutes bars request
    public static final int HANDLER_TICKS_TO_SECOND_BARS = 2; // handler ID for the data handler for second bars request
    public static final int HANDLER_DAY_BARS = 3; // handler ID for the data handler for day bars request

    public static final String NAME = "IQFeed";

    /**
     * Supports doing synchronous calls for requesting and getting daily data for symbols.
     */
    private AsyncFunctionFutureResults<Bar> dailyDataPendingCalls = null;
    private AsyncFunctionFutureResults<List<TimeSlotBarWithTimeStamp>> barsRequestPendingCalls = null;
    private ConcurrentHashMap<String, Integer> tagToHandlerIdMap = null;  // maps the callback tag for async responses to data handler IDs
    private ConcurrentHashMap<String, ArrayList<String[]>> pendingLookupData; // holds tagged lookup data that has not been completed yet (no !ENDMSG! seen)

    private AsyncFunctionFutureResults<Fundamentals> fundamentalsDataPendingCalls = null;
    private AtomicBoolean fundamentalsRequested;
    private boolean hadFatalError = false;
    private boolean wasRestartRequested = false;

    private PrintWriter timingLogFile = null;

    // staleness stats
    public static final int BOUNDARY_LATENCY_ALERT_LIMIT = 300;
    public static final String IQFEED_LATENCY = "IQFeed latency";
    private static final int STALE_THRESHOLD_ON_TICK_PROCESSING = 500;

    public static final String STAT_DESCRIPTIVE_STALE_AGE = "staleAge";
    public static final String STAT_DESCRIPTIVE_OUT_OF_ORDER_AGE = "outOfOrderAge";
    public static final String STAT_TOTAL_STALE_TICKS = "totalStaleTicks";
    public static final String STAT_LAST_STALE_TICK_AGE = "lastStaleTickAge";
    public static final String STAT_TOTAL_OUT_OF_ORDER_NOT_CONVERTED = "totalOutOfOrderNotConverted";
    public static final String STAT_TOTAL_OUT_OF_ORDER_CONVERTED = "totalOutOfOrderConverted";
    public static final int MAX_STATS_PUBLIHING_FREQUENCY = 1000; // msec no more frequent than that

    private int timeMessageRecievedAt = 0;
    private long timeMessageCount = 0;
    private final Object staleStatsMonitor = new Object();
    private AtomicLong totalTicks = new AtomicLong(0); // not using stats service due to high frequency
    private AtomicLong lastTickLatency = new AtomicLong(0); // not using stats service due to high frequency

    private AtomicInteger lastPublishedStatsTimestamp = new AtomicInteger(0);
    private String lastStaleTickSymbol = "";
    private String lastOutOfOrderTickSymbol = "";
    private Calendar lastHiccupTimeStamp = null;
    private int lastHiccupDuration = 0;
    private Calendar lastStaleTickTimeStamp = null;

    private long lastMemStatsSampleTs = 0;
    public static final long MEMORY_SAMPLE_MIN_INTERVAL = 1000; // 1 second
    public static final int MEMSTATS_BUFFER_SIZE = 1200; // 20 min
    private MemoryStateHistory memStats;
    private StaleTickLogger staleTickLogger;

    public DtnIQFeedClient() {
        timeMessageRecievedAt = TimeHelper.now() - TimeHelper.now() % 1000;
        daemonsShouldBeRunning = new AtomicBoolean(true);
        isWatchingSymbols = new AtomicBoolean(false);
        facadeInitializationTimeoutLock = new ReentrantLock();
        connectionConfirmedCondition = facadeInitializationTimeoutLock.newCondition();
        connectionConfirmed = new AtomicBoolean(false);
        isConnected = new AtomicBoolean(false);

        rawMessageQueue = new LinkedBlockingQueue<MessageQueueItem<String>>();
        rawMessageQueueSize = new AtomicInteger(0);
        tickQueue = new ConcurrentLinkedQueue<RealTimeTradeRecord>();
        tickQueueSize = new AtomicInteger(0);
        fieldPositions = new ConcurrentHashMap<String, Integer>();
        tradeTickCounter = new AtomicInteger(0);
        nonTradeTickCounter = new AtomicInteger(0);

        fundamentalFieldPositions = new ConcurrentHashMap<String, Integer>();
        fundamentals = new ConcurrentHashMap<String, Map>();

        blockingWaitLock = new ReentrantLock();
        blockingWaitNewDataAvailable = blockingWaitLock.newCondition();
        blockingWaitCounter = new AtomicInteger(0);

        maxMillisBetweenTicks = DEFAULT_HICCUP_INTERVAL_MILLIS;
        lastObservedTickTs = new AtomicInteger(-1); // not yet observed  (used only for hiccup detection - not time stamping)

        maxObservedTickTimestamp = new AtomicInteger(-1); // not observed

        tagToHandlerIdMap = new ConcurrentHashMap<String, Integer>();
        pendingLookupData = new ConcurrentHashMap<String, ArrayList<String[]>>();
        fundamentalsRequested = new AtomicBoolean(false);
    }

    @Override
    public void onConnectionEstablished() {
        //fixme implement DtnIqfeedHistoricalClient.onConnectionEstablished()
    }

    @Override
    public void onConnectionLost() {
        //fixme implement DtnIqfeedHistoricalClient.onConnectionLost()
    }

    @Override
    public void onConnectionTerminated() {
        //fixme implement DtnIqfeedHistoricalClient.onConnectionTerminated()
    }

    @Override
    public void onException(Throwable ex, boolean wasHandled) {
        //fixme implement DtnIqfeedHistoricalClient.onException()
    }

    @Override
    public void onAdminData(String data) {
        //fixme implement DtnIqfeedHistoricalClient.onAdminData()
    }

    @Override
    public void onLevelOneData(String data) {
        //fixme implement DtnIqfeedHistoricalClient.onLevelOneData()
    }

    @Override
    public void onFatalException(Throwable e) {
        //fixme implement DtnIqfeedHistoricalClient.onFatalException()
    }

    @Override
    public void onLookupData(String rawMessages) {
        //fixme implement DtnIqfeedHistoricalClient.onLookupData()
    }

    @Override
    public void init(IDtnIQFeedConfig iDtnIQFeedConfig) throws InvalidArgumentException, InvalidStateException {
        //fixme implement DtnIqfeedHistoricalClient.init()
    }

    // ==============================================================================================================
    protected class Fundamentals {
            // form of "Split Factor 1" and "Split Factor 2": 0.50 07/09/2010,0.50 12/22/1997
            private long commonShares;
            private float yearHigh;
            private float yearLow;
            private long averageVolume;
            private String splitFactor1;
            private String splitFactor2;
            private float splitFactor1factor;
            private float splitFactor2factor;
            private String splitFactor1date;
            private String splitFactor2date;

            @Override
            public String toString() {
                StringBuilder builder = new StringBuilder();
                builder.append("common shares: ").append(commonShares).append(",");
                builder.append("52 week high: ").append(yearHigh).append(",");
                builder.append("52 week low: ").append(yearLow);
                return super.toString();
            }

            public long getCommonShares() {
                return commonShares;
            }

            public void setCommonShares(long commonShares) {
                this.commonShares = commonShares;
            }

            public float getYearHigh() {
                return yearHigh;
            }

            public void setYearHigh(float yearHigh) {
                this.yearHigh = yearHigh;
            }

            public float getYearLow() {
                return yearLow;
            }

            public void setYearLow(float yearLow) {
                this.yearLow = yearLow;
            }

            public long getAverageVolume() {
                return averageVolume;
            }

            public void setAverageVolume(long averageVolume) {
                this.averageVolume = averageVolume;
            }

            public String getSplitFactor1() {
                return splitFactor1;
            }

            public String getSplitFactor1date() {
                return splitFactor1date;
            }

            public float getSplitFactor1factor() {
                return splitFactor1factor;
            }

            public String getSplitFactor2() {
                return splitFactor2;
            }

            public String getSplitFactor2date() {
                return splitFactor2date;
            }

            public float getSplitFactor2factor() {
                return splitFactor2factor;
            }

            public void setSplitFactor1(String splitFactor)  {
                this.splitFactor1 = splitFactor;
                if (StringUtils.isBlank(splitFactor)) {
                    return;
                }
                splitFactor1factor = extractFactor(splitFactor);
                splitFactor1date = extractDate(splitFactor);
            }

            public boolean hasSplitFactor1() {
                return StringUtils.isNotBlank(splitFactor1);
            }

            public boolean hasSplitFactor2() {
                return StringUtils.isNotBlank(splitFactor2);
            }

            public void setSplitFactor2(String splitFactor)  {
                this.splitFactor2 = splitFactor;
                if (StringUtils.isBlank(splitFactor)) {
                    return;
                }
                splitFactor2factor = extractFactor(splitFactor);
                splitFactor2date = extractDate(splitFactor);
            }

            private String extractDate(String splitFactor)  {
                String[] parts = splitFactor.split(" +");
                return parts[1].trim();
            }

            private float extractFactor(String splitFactor) {
                String[] parts = splitFactor.split(" +");
                return Float.parseFloat(parts[0].trim());
            }

            public Pair<Calendar,Float> getLastSplit() throws InvalidArgumentException {
                Calendar date1=null;
                Float ratio1 = null;
                Calendar date2=null;
                Float ratio2 = null;
                if (StringUtils.isNotBlank(getSplitFactor1date())) {
                    date1 = TimeHelper.parseMmDdYyyyDate(getSplitFactor1date());
                    ratio1 = getSplitFactor1factor();
                }
                if (StringUtils.isNotBlank(getSplitFactor2date())) {
                    date1 = TimeHelper.parseMmDdYyyyDate(getSplitFactor2date());
                    ratio1 = getSplitFactor2factor();
                }
                if (date1==null && date2==null) {
                    return null;
                }

                if (date1!=null && date2==null) {
                    return new Pair<Calendar, Float>(date1,ratio1);
                }

                if (date1==null && date2!=null) {
                    return new Pair<Calendar, Float>(date2,ratio2);
                }

                // both not null - pick the later one
                if (date1.compareTo(date2) > 0) {
                    return new Pair<Calendar, Float>(date1,ratio1);
                }
                else {
                    return new Pair<Calendar, Float>(date2,ratio2);
                }
            }
        }
}
