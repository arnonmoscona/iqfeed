package com.moscona.trading.adapters.iqfeed;

import com.moscona.events.EventPublisher;
import com.moscona.threads.RealTimeProviderConnectionThread;
import com.moscona.trading.IServicesBundle;
import com.moscona.trading.ServicesBundle;
import com.moscona.trading.adapters.DataSourceCoreStats;
import com.moscona.trading.excptions.MissingSymbolException;
import com.moscona.trading.streaming.HeavyTickStreamRecord;
import com.moscona.util.ISimpleDescriptiveStatistic;
import com.moscona.util.SafeRunnable;
import com.moscona.util.async.AsyncFunctionFutureResults;
import com.moscona.exceptions.InvalidArgumentException;
import com.moscona.exceptions.InvalidStateException;
import com.moscona.trading.elements.Bar;
import com.moscona.trading.streaming.TimeSlotBarWithTimeStamp;
import com.moscona.util.TimeHelper;
import com.moscona.util.collections.Pair;
import com.moscona.util.monitoring.MemoryStateHistory;
import com.moscona.util.monitoring.stats.IStatsService;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.io.File;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
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
    //public static final int DEFAULT_HICCUP_INTERVAL_MILLIS = 500;
    public static final String IQFEED_NO_DATA_RESPONSE = "!NO_DATA!";
    public static final int MAX_STATS_PUBLIHING_FREQUENCY = 1000; // msec no more frequent than that


    public static final String TAG_PREFIX_31_DAY = "31 day ";
    public static final int HANDLER_31_DAY_BARS = 0; // handler ID for the data handler for 31 day bars request
    public static final int HANDLER_MINUTE_BARS = 1; // handler ID for the data handler for minutes bars request
    public static final int HANDLER_TICKS_TO_SECOND_BARS = 2; // handler ID for the data handler for second bars request
    public static final int HANDLER_DAY_BARS = 3; // handler ID for the data handler for day bars request

    public static final String NAME = "IQFeed";

    private IDtnIQFeedFacade facade = null;
    private boolean initialized = false;
    private boolean isShutDown = false;

    private AtomicBoolean daemonsShouldBeRunning;
    private Thread heartBeatThread = null; // guarantees some code gets executed at a regular frequency
    private AtomicBoolean isWatchingSymbols;

    private int initialConnectionTimeoutMillis;
    private final ReentrantLock facadeInitializationTimeoutLock;
    private Condition connectionConfirmedCondition;
    private AtomicBoolean connectionConfirmed; // needed for double-checking
    private AtomicBoolean isConnected;


    private ConcurrentHashMap<String, Integer> fieldPositions;
    private ConcurrentHashMap<String, Map> fundamentals;
    private ConcurrentHashMap<String, Integer> fundamentalFieldPositions;
    private final ReentrantLock blockingWaitLock;         // lock for the following condition

    private AtomicInteger rawMessageQueueSize;                    // counting so that we don't have to call size() on the queue
    private LinkedBlockingQueue<MessageQueueItem<String>> rawMessageQueue;          // raw data without pre-processing as it came from the facade
    private final Condition blockingWaitNewDataAvailable; // condition that there is new data available
    private AtomicInteger blockingWaitCounter;            // counts how many times we had blocking waits requested

    private IDtnIQFeedConfig config = null;
    private IServicesBundle defaultServiceBundle=null;
    private IServicesBundle parserDaemonServiceBundle=null;
    private IServicesBundle lookupThreadServiceBundle=null;
    private IServicesBundle heartBeatServicesBundle;

    boolean debugFlag = false;
    String debugTag = "not set";

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
//    public static final int BOUNDARY_LATENCY_ALERT_LIMIT = 300;
//    public static final String IQFEED_LATENCY = "IQFeed latency";
//    private static final int STALE_THRESHOLD_ON_TICK_PROCESSING = 500;
//
//    public static final String STAT_DESCRIPTIVE_STALE_AGE = "staleAge";
//    public static final String STAT_DESCRIPTIVE_OUT_OF_ORDER_AGE = "outOfOrderAge";
//    public static final String STAT_TOTAL_STALE_TICKS = "totalStaleTicks";
//    public static final String STAT_LAST_STALE_TICK_AGE = "lastStaleTickAge";
//    public static final String STAT_TOTAL_OUT_OF_ORDER_NOT_CONVERTED = "totalOutOfOrderNotConverted";
//    public static final String STAT_TOTAL_OUT_OF_ORDER_CONVERTED = "totalOutOfOrderConverted";
//    public static final int MAX_STATS_PUBLIHING_FREQUENCY = 1000; // msec no more frequent than that

    private int timeMessageRecievedAt = 0;
    private long timeMessageCount = 0;
    //private final Object staleStatsMonitor = new Object();
    //private AtomicLong totalTicks = new AtomicLong(0); // not using stats service due to high frequency
    //private AtomicLong lastTickLatency = new AtomicLong(0); // not using stats service due to high frequency

    private AtomicInteger lastPublishedStatsTimestamp = new AtomicInteger(0);
//    private String lastStaleTickSymbol = "";
//    private String lastOutOfOrderTickSymbol = "";
//    private Calendar lastHiccupTimeStamp = null;
//    private int lastHiccupDuration = 0;
//    private Calendar lastStaleTickTimeStamp = null;

    private long lastMemStatsSampleTs = 0;
    public static final long MEMORY_SAMPLE_MIN_INTERVAL = 1000; // 1 second
    public static final int MEMSTATS_BUFFER_SIZE = 1200; // 20 min
    private MemoryStateHistory memStats;
    private int maxSymbols;
    private int watchedSymbols;
    private String iqFeedVersion;
    private float currentInternetBandwidthConsumption;
    private float currentLocalBandwidthConsumption;


//    private StaleTickLogger staleTickLogger;

    public DtnIqfeedHistoricalClient() {
        timeMessageRecievedAt = TimeHelper.now() - TimeHelper.now() % 1000;
        daemonsShouldBeRunning = new AtomicBoolean(true);
        isWatchingSymbols = new AtomicBoolean(false);
        facadeInitializationTimeoutLock = new ReentrantLock();
        connectionConfirmedCondition = facadeInitializationTimeoutLock.newCondition();
        connectionConfirmed = new AtomicBoolean(false);
        isConnected = new AtomicBoolean(false);

        rawMessageQueue = new LinkedBlockingQueue<MessageQueueItem<String>>();
        rawMessageQueueSize = new AtomicInteger(0);

//        tickQueue = new ConcurrentLinkedQueue<RealTimeTradeRecord>();
//        tickQueueSize = new AtomicInteger(0);
        fieldPositions = new ConcurrentHashMap<String, Integer>();
//        tradeTickCounter = new AtomicInteger(0);
//        nonTradeTickCounter = new AtomicInteger(0);

        fundamentalFieldPositions = new ConcurrentHashMap<String, Integer>();
        fundamentals = new ConcurrentHashMap<String, Map>();

        blockingWaitLock = new ReentrantLock();
        blockingWaitNewDataAvailable = blockingWaitLock.newCondition();
        blockingWaitCounter = new AtomicInteger(0);

//        maxMillisBetweenTicks = DEFAULT_HICCUP_INTERVAL_MILLIS;
//        lastObservedTickTs = new AtomicInteger(-1); // not yet observed  (used only for hiccup detection - not time stamping)
//
//        maxObservedTickTimestamp = new AtomicInteger(-1); // not observed

        tagToHandlerIdMap = new ConcurrentHashMap<String, Integer>();
        pendingLookupData = new ConcurrentHashMap<String, ArrayList<String[]>>();
        fundamentalsRequested = new AtomicBoolean(false);
    }

    //<editor-fold desc="Initialization, heartbeet, stats publishing">
    /**
     * A method which when called on an instance produced by a default constructor, would either make this instance
     * fully functional, or would result in an exception indicating that the instance should be thrown away.
     *
     * @param config an instance of the server configuration to work with.
     * @throws com.moscona.exceptions.InvalidArgumentException
     *          if any of the relevant configuration entries is wrong
     * @throws com.moscona.exceptions.InvalidStateException
     *          if after trying everything else, was still in an invalid state.
     */
    @Override
    public void init(IDtnIQFeedConfig config) throws InvalidArgumentException, InvalidStateException {
        this.config = config;
        try {
//            debug("initialization","started");
            Map myConfig = (Map)config.getComponentConfigFor("DtnIQFeedClient");
            defaultServiceBundle = config.getServicesBundle();
            parserDaemonServiceBundle = config.createServicesBundle();
            lookupThreadServiceBundle = config.createServicesBundle();

//            preTradingMonitoringStartMillis = (Integer)myConfig.get("preTradingMonitoringStartMinutes")* MILLIS_PER_MINUTE;
//            TradingHours tradingHours = new TradingHours(config);
//            startWatchingSymbolsTimestamp = tradingHours.getStartTradingTimeMillis() - preTradingMonitoringStartMillis;
//            endTradingDayTimestamp = tradingHours.getEndTradingTimeMillis();

//            maxMillisBetweenTicks = (Integer)myConfig.get("maxMillisBetweenTicks");

            initialConnectionTimeoutMillis = (Integer)myConfig.get("connectionTimeoutMillis");
            if (initialConnectionTimeoutMillis < 1) {
                throw new InvalidArgumentException("The connectionTimeoutMillis parameter must be a positive integer");
            }

//            marketTree = config.getMarketTree();
//            if (marketTree==null) {
//                throw new InvalidArgumentException("Market tree is required for the IQFeed client");
//            }

            startMessageParserDaemon();

            initFacade(config, myConfig);

//            config.getEventPublisher().onEvent(EventPublisher.Event.TRADING_CLOSED, new SafeRunnable() {
//                @Override
//                public void runWithPossibleExceptions() throws Exception {
//                    stopWatchingSymbols();
//                }
//            }.setName("IQFeed stop watching symbols"));

            heartBeatServicesBundle = config.createServicesBundle();
            startHeartBeat();
            listenForStatsUpdateRequests();

//            staleTickLogger = new StaleTickLogger(config);

            initialized = true;
//            debug("initialization","complete");
        } catch (NumberFormatException e) {
//            debug("Exception", "NumberFormatException "+e);
            throw new InvalidArgumentException("error while initializing IQFeed client: "+e, e);
        } catch (ClassCastException e) {
//            debug("Exception", "ClassCastException "+e);
            throw new InvalidArgumentException("error while initializing IQFeed client: "+e, e);
        } catch (Throwable e) {
//            debug("Exception", e.getClass().getName()+" "+e);
            throw new InvalidStateException("error while initializing IQFeed client: "+e, e);
        }
    }

    /**
     * Starts the heartbeat thread
     */
    @SuppressWarnings({"unchecked"})
    private void startHeartBeat() {
        heartBeatThread = new RealTimeProviderConnectionThread(new SafeRunnable() {
            @Override
            public void runWithPossibleExceptions() throws Exception {
                while (daemonsShouldBeRunning.get() && !Thread.currentThread().isInterrupted()) {
                    try {
                        onHeartBeat();
                    } catch (InvalidStateException e) {
                        getHeartBeatServicesBundle().getAlertService().sendAlert("Exception during IQFeed client heartbeat: " + e, IQFEED_ERROR_ALERT_MESSAGE_TYPE, e);
                    }
                    Thread.sleep(HEARTBEAT_SLEEP_MILLIS);
                }
            }
        }, "IQFeed client heartbeat", config);
        heartBeatThread.start();
    }

    /**
     * Called once every heartbeat
     *
     * @throws com.moscona.exceptions.InvalidStateException
     */
    private void onHeartBeat() throws InvalidStateException {
        //autoStartTradingDay();
        monitorRawMessageQueueSize(rawMessageQueueSize.get());
        //monitorForHiccups();
        collectStatsCounters();
    }

    /**
     * Collects stats on raw message queue size and issues alerts when it gets too long
     *
     * @param size
     */
    private void monitorRawMessageQueueSize(int size) {
        //FIXME implement DtnIQFeedClient.monitorRawMessageQueueSize
    }

    private void collectStatsCounters() throws InvalidStateException {
        final IStatsService stats = getHeartBeatServicesBundle().getStatsService();
        String prefix = "IQFeed client ";
        stats.setStat(prefix + "rawMessageQueueSize", rawMessageQueueSize.get());
//        stats.setStat(prefix + "tickQueueSize", tickQueueSize.get());
        stats.setStat(prefix + "blockingWaitCounter", blockingWaitCounter.get());
//        stats.setStat(prefix + "tradeTickCounter", tradeTickCounter.get());
//        stats.setStat(prefix + "nonTradeTickCounter", nonTradeTickCounter.get());
//        stats.setStat(prefix + "lastObservedTickTs", lastObservedTickTs.get());
//        stats.setStat(prefix + "maxObservedTickTimestamp", maxObservedTickTimestamp.get());
        stats.setStat(prefix + "currentLocalBandwidthConsumption", currentLocalBandwidthConsumption);
        stats.setStat(prefix + "currentInternetBandwidthConsumption", currentInternetBandwidthConsumption);
//        synchronized (staleStatsMonitor) {
//            IStatsService parserStats = parserDaemonServiceBundle.getStatsService();
//            prefix += "staleness ";
//            stats.setStat(prefix + "staleTickCounter", parserStats.getStat(STAT_TOTAL_STALE_TICKS).getLong());
//            stats.setStat(prefix + "staleTickLastAge", parserStats.getStat(STAT_LAST_STALE_TICK_AGE).getLong());
//            ISimpleDescriptiveStatistic desc = parserStats.getStat(STAT_DESCRIPTIVE_STALE_AGE).getDescriptiveStatistics();
//            stats.setStat(prefix + "staleTickAverageAge", desc.average());
//            stats.setStat(prefix + "staleTickMaxAge", desc.max());
//            stats.setStat(prefix + "staleTickMinAge", desc.min());
//            stats.setStat(prefix + "staleTickStdevAge", desc.stdev());
//
//            stats.setStat(prefix + "outOfOrderTickCounter", parserStats.getStat(STAT_TOTAL_OUT_OF_ORDER_NOT_CONVERTED).getLong());
//            stats.setStat(prefix + "outOfOrderTicksCurried", parserStats.getStat(STAT_TOTAL_OUT_OF_ORDER_CONVERTED).getLong());
//            desc = parserStats.getStat(STAT_DESCRIPTIVE_OUT_OF_ORDER_AGE).getDescriptiveStatistics();
//            stats.setStat(prefix + "outOfOrderTickAverageAge", desc.average());
//            stats.setStat(prefix + "outOfOrderTickMaxAge", desc.max());
//            stats.setStat(prefix + "outOfOrderTickMinAge", desc.min());
//            stats.setStat(prefix + "outOfOrderTickStdevAge", desc.stdev());
//        }
    }

    private void listenForStatsUpdateRequests() {
        config.getEventPublisher().onEvent(EventPublisher.Event.STATS_UPDATE_REQUEST, new SafeRunnable() {
            @Override
            public void runWithPossibleExceptions() throws Exception {
                publishStatsUpdate();
            }
        }.setName("IQFeed stats update request listener"));

        config.getEventPublisher().onEvent(EventPublisher.Event.STALE_STATS_RESET_REQUEST, new SafeRunnable() {
            @Override
            public void runWithPossibleExceptions() throws Exception {
                resetStaleStats();
            }
        }.setName("IQFeed staleStats reset responder"));

        config.getEventPublisher().onEvent(EventPublisher.Event.DUMP_MEMORY_TRACKER_REQUEST, new SafeRunnable() {
            @Override
            public void runWithPossibleExceptions() throws Exception {
                dumpMemStats();
            }
        }.setName("IQFeed DUMP_MEMORY_TRACKER_REQUEST responder"));
    }

    protected  void dumpMemStats() {
//        if (memStats == null) {
//            return;
//        }
//        try {
//            // figure out the file
//            String marketTreeArchivePath = config.getLocalStore().getArchivedMarketTreePath(TimeHelper.nowAsCalendar());
//            String dumpFilePath = new File(marketTreeArchivePath).getParent() + "/memSampleHistory.csv";
//
//            // request a dump
//            memStats.print(new File(dumpFilePath));
//        } catch (Exception e) {
//            config.getAlertService().sendAlert("Failed to dump memory stats in IQFeed adapeter: " + e, e);
//        }
    }

    protected void resetStaleStats() {
//        synchronized (staleStatsMonitor) {
//            IStatsService stats = parserDaemonServiceBundle.getStatsService();
//            stats.initStatWithDescriptiveStats(STAT_DESCRIPTIVE_STALE_AGE, 0);
//            stats.setStat(STAT_TOTAL_STALE_TICKS, 0);
//            stats.setStat(STAT_LAST_STALE_TICK_AGE, 0);
//            stats.setStat(STAT_TOTAL_OUT_OF_ORDER_NOT_CONVERTED, 0);
//            stats.setStat(STAT_TOTAL_OUT_OF_ORDER_CONVERTED, 0);
//            stats.initStatWithDescriptiveStats(STAT_DESCRIPTIVE_OUT_OF_ORDER_AGE, 0);
//            lastStaleTickSymbol = "";
//            lastOutOfOrderTickSymbol = "";
//        }
    }

    protected void publishStatsUpdate() {
        int now = TimeHelper.now();
        int publishInterval = now - lastPublishedStatsTimestamp.get();
        if (publishInterval >= 0 && publishInterval < MAX_STATS_PUBLIHING_FREQUENCY) {
            // if < 0 then it was basically yesterday
            // if positive but less than the maximum publish frequency - just ignore the request
            return;
        }

        try {
            try {
                collectStatsCounters();
            } catch (Exception e) {
                config.getAlertService().sendAlert("Exception while trying to report stats, collectStatsCounters(): " + e);
                return; // ignore request
            }

            IStatsService stats = getHeartBeatServicesBundle().getStatsService();
            float avgStaleness = (float) stats.getStat("IQFeed client staleness staleTickAverageAge").getDouble();
            int lastStaleTickAge = (int) stats.getStat("IQFeed client staleness staleTickLastAge").getLong();
            long totalTicks = stats.getStat("IQFeed client tradeTickCounter").getLong();
            long totalStaleTicks = stats.getStat("IQFeed client staleness staleTickCounter").getLong();
            float percentStaleTicks = (100.0f * totalStaleTicks) / totalTicks;
            long outOfOrderTicks = stats.getStat("IQFeed client staleness outOfOrderTickCounter").getLong();
            float bwConsumption = (float) stats.getStat("IQFeed client currentInternetBandwidthConsumption").getDouble();
            int tickQueueSize = (int) stats.getStat("IQFeed client tickQueueSize").getLong();

            DataSourceCoreStats retval = createDataSourceCoreStats(avgStaleness, lastStaleTickAge, totalStaleTicks, percentStaleTicks, outOfOrderTicks, bwConsumption, tickQueueSize);  // IMPORTANT: real time data excluded
            retval.publish(config.getEventPublisher());
        } catch (Exception e) {
            config.getAlertService().sendAlert("Exception while trying to report stats: " + e, e);
        } finally {
            lastPublishedStatsTimestamp.set(TimeHelper.now());
        }
    }

    protected DataSourceCoreStats createDataSourceCoreStats(float avgStaleness, int lastStaleTickAge, long totalStaleTicks, float percentStaleTicks, long outOfOrderTicks, float bwConsumption, int tickQueueSize) {
        return new DataSourceCoreStats(avgStaleness, lastStaleTickAge,
                outOfOrderTicks, percentStaleTicks, getRawMessageQueueSize(),
                -1, totalStaleTicks, tickQueueSize, bwConsumption, "",
                null, -1, null, "");
    }
    //</editor-fold>

    //<editor-fold desc="Connection lifecycle events">
    @Override
    /**
     * Called when the facade completed the connection setup and is ready to receive commands
     */
    public void onConnectionEstablished() {
        facadeInitializationTimeoutLock.lock();
        try {
            facade.onConnectionEstablished();
            connectionConfirmed.set(true);
            isConnected.set(true);
            connectionConfirmedCondition.signalAll();
        } finally {
            facadeInitializationTimeoutLock.unlock();
        }
    }

    @Override
    /**
     * Called when a connection that is supposed to be up is lost
     */
    public void onConnectionLost() {
        if (!isConnected.get()) {
            return; // didn't think I was connected anyway
        }
        sendParserAlert("Lost connection", null);
        isConnected.set(false);
        sendParserAlert("Trying to reconnect", null);
        try {
            facade.requestConnect();
        } catch (InvalidStateException e) {
            sendParserAlert("Exception while trying to reconnect: " + e, e);
            onException(e, false);
        }
    }

    @Override
    public void onConnectionTerminated() {
        //fixme implement DtnIqfeedHistoricalClient.onConnectionTerminated()
    }
    //</editor-fold>

    @Override
    /**
     * Called whenever the facade encounters an exception.
     *
     * @param ex         the exception
     * @param wasHandled whether or not it was handled already
     */ public void onException(Throwable ex, boolean wasHandled) {
        //fixme implement DtnIqfeedHistoricalClient.onException()
    }

    @Override
    /**
     * Called when the facade receives new data from the admin connection. This is typically more than one message.
     *
     * @param data the data as a String
     */
    public void onAdminData(String data) {
        sampleMemory(data.length());
        //        debug("got data",data);
        rawMessageQueue.add(new MessageQueueItem<String>(data));
        int queueSize = rawMessageQueueSize.incrementAndGet();
        //        debug("queue size",""+queueSize);
    }

    @Override
    /**
     * Called when the facade receives new data from the level1 connection. This is typically more than one message.
     *
     * @param data the data as a String
     */
    public void onLevelOneData(String data) {
        onAdminData(data); // right now there is no reason to use separate structures on these
    }

    @Override
    /**
     * A callback for the facade to notify the client that something terrible happened and everything needs to be shut down.
     *
     * @param e
     */
    public void onFatalException(Throwable e) {
        try {
            shutdown();
        } catch (InvalidStateException e1) {
            defaultServiceBundle.getAlertService().sendAlert("Error while shutting down IQFeed: " + e, e);
        }
    }

    /**
     * Notifies the data source that a shutdown is in progress and instructs it to shut down all connections
     */
    public void shutdown() throws InvalidStateException {
        if (initialized && !isShutDown) {
            facade.stopIQConnect();
            stopDaemonThreads();
            isShutDown = true;
            isConnected.set(false);
        }
    }

    /**
     * Stop all supporting threads
     */
    public void stopDaemonThreads() {
        daemonsShouldBeRunning.set(false);
    }

    @Override
    /**
     * A callback for the facade to notify the client about new data received on the lookup port
     *
     * @param rawMessages
     */
    public void onLookupData(String rawMessages) {
        // at this point we basically have a list of tagged line (each line) which all need to be added to their
        // pending lists until we see a line with tag,!ENDMSG! - at which point we need to route the whole list to its
        // handler and remove it from pending
        if (StringUtils.isBlank(rawMessages)) {
            return; // nothing to do
        }
        String[] lines = StringUtils.split(rawMessages, "\n");
        for (String line : lines) {
            String[] fields = StringUtils.split(line, ",");
            if (fields.length == 0) {
                continue;
            }
            String tag = fields[0];
            if (! tagToHandlerIdMap.containsKey(tag)) {
                sendLookupAlert("Unregistered tag in response from lookup (ignoring): "+tag, IQFEED_LOOKUP_ERROR_ALERT_MESSAGE_TYPE);
                continue;
            }

            ArrayList<String[]> list = pendingLookupData.get(tag);
            getBarsRequestPendingCalls().markFirstResponseTimeStamp(tag); // track time to first byte for the tag

            if (fields[1].trim().equals("!ENDMSG!")) {
                removeFromPendingAndHandle(tag,list);
            }
            else {
                // add to pending and continue
                if (list==null) {
                    list = new ArrayList<String[]>();
                    pendingLookupData.put(tag,list);
                }
                list.add(fields);
            }
        }
    }


    // ==============================================================================================================
    // Lookup handling
    // ==============================================================================================================

    //<editor-fold desc="Lookup handling">

    private void removeFromPendingAndHandle(String tag, ArrayList<String[]> list) {
        long start = System.currentTimeMillis();
        getBarsRequestPendingCalls().markLastByteTimeStamp(tag);
        getBarsRequestPendingCalls().setDataSize(tag, list.size());
        try {
            pendingLookupData.remove(tag);
            int handlerId = tagToHandlerIdMap.get(tag);
            switch (handlerId) {
                case HANDLER_31_DAY_BARS:
                    handle31DayBars(list);
                    break;
                case HANDLER_MINUTE_BARS:
                    handleMinuteBars(list);
                    break;
                case HANDLER_TICKS_TO_SECOND_BARS:
                    handleTicksToSecondBars(list);
                    break;
                case HANDLER_DAY_BARS:
                    handleDayBars(list);
                    break;
                default:
                    sendLookupAlert("Unknown handler for tag '" + tag + "': " + handlerId, IQFEED_LOOKUP_ERROR_ALERT_MESSAGE_TYPE);
            }
        } finally {
            long processingTime = System.currentTimeMillis() - start;
            getBarsRequestPendingCalls().setPostResponseProcessingTime(tag, processingTime);
        }
    }

    /**
     * A method that handles minute bars from HIT requests
     * @param list
     */
    @SuppressWarnings({"ThrowableInstanceNeverThrown"})
    private void handleDayBars(ArrayList<String[]> list) {
        handleIQFeedBars(list, false, -24, Calendar.HOUR, "lookup_day_bars");
    }

    @SuppressWarnings({"ThrowableInstanceNeverThrown"})
    private void handle31DayBars(ArrayList<String[]> list) {
        // FIXME This should be factored out as a part of a pluggable add-on
        if (list.size() == 0) {
            return; // nothing to do and don't know how to tell any pending call
        }
        // BIDU daily,2010-05-10 00:00:00,69.4790,66.7440,66.7450,69.4780,1724987,0,
        // missing symbol error:
        //   Response: bogus daily,E,Invalid symbol.,
        // tag,date,high,low,open,close,total volume,period volume
        // http://www.iqfeed.net/dev/api/docs/HistoricalviaTCPIP.cfm
        // returns data via dailyDataPendingCalls
        String[] first = list.get(0);
        String tag = first[0];

        // Check for error message
        if (first[1].equals("E")) {
            String symbol = tag.replaceFirst(TAG_PREFIX_31_DAY, "").trim();
            dailyDataPendingCalls.throwException(tag, new MissingSymbolException(symbol, first[2], "IQFeed reported error: " + first[2]));
        }

        if (list.size() < 1) {
            String message = "handle31DayBars(): Expected at least 1 data point.";
            sendLookupAlert(message, IQFEED_LOOKUP_ERROR_ALERT_MESSAGE_TYPE);
            dailyDataPendingCalls.throwException(tag, new Exception(message));
            return;
        }

        String[] currentLine = null;
        try {
            long sum = 0;
            Bar retval = null;
            for (String[] fields : list) {

                if (fields.length < 7) {
                    throw new Exception("Error while parsing raw response line. Insufficient number of fields: " + StringUtils.join(fields, ','));
                }
                // figure out the date for this
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd"); // reall ha also " hh:mm:ss" but we ignore that here
                String[] timestampParts = StringUtils.split(fields[1], " ");
                Calendar date = Calendar.getInstance();
                date.setTime(format.parse(timestampParts[0]));
                Calendar today = TimeHelper.today();
                if (DateUtils.isSameDay(today, date)) {
                    continue; // ignore data from today. We want yesterday's close
                }


                currentLine = fields;
                float high = Float.parseFloat(fields[2]);
                float low = Float.parseFloat(fields[3]);
                float open = Float.parseFloat(fields[4]);
                float close = Float.parseFloat(fields[5]);
                int volume = Integer.parseInt(fields[6]);
                sum += volume;
                if (retval == null) {
                    retval = new Bar(open, close, high, low, volume);
                } else {
                    retval.setOpen(open); // we're going backward in time
                    retval.setHigh(Math.max(high, retval.getHigh()));
                    retval.setLow(Math.min(low, retval.getLow()));
                }
                dailyDataPendingCalls.returnToCallers(tag, retval, config.getLookupStatsService(), "handle_31day_bars");
                return;
            }
            if (retval == null) {
                throw new Exception("handle31DayBars(): expected return value to have a value at this point, but got a null...");
            }
        } catch (Exception e) {
            // cause the calling function to throw an exception
            dailyDataPendingCalls.throwException(tag, new Exception("handle31DayBars(): Error while processing line: " + StringUtils.join(currentLine, ',') + " :" + e, e), config.getLookupStatsService(), "handle_31day_bars");
        }
    }

    /**
     * A method that handles minute bars from HIT requests
     * @param list
     */
    @SuppressWarnings({"ThrowableInstanceNeverThrown"})
    private void handleMinuteBars(ArrayList<String[]> list) {
        // FIXME This should be factored out as a part of a pluggable add-on
        handleIQFeedBars(list, true, -1, Calendar.MINUTE, "lookup_1min_bars");
    }

    private void handleIQFeedBars(ArrayList<String[]> list, boolean useSecondVolumeNumber, int unitIncrement, int unit, String prefix) {
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


        AsyncFunctionFutureResults<List<TimeSlotBarWithTimeStamp>> pending = getBarsRequestPendingCalls();

        // Check for error message
        if (first[1].equals("E")) {
            String literalError = first[2];
            if (literalError.equals(IQFEED_NO_DATA_RESPONSE)) {
                // make an empty response
                pending.returnToCallers(tag, new ArrayList<TimeSlotBarWithTimeStamp>(), config.getLookupStatsService(), prefix);
                return; // and there's nothing else to do here
            }

            boolean canRetry = canRetry(literalError);
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
                Calendar timestamp = TimeHelper.parse(timestampString);
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

            pending.returnToCallers(tag, retval, config.getLookupStatsService(), prefix);
        } catch (Exception e) {
            pending.throwException(tag, new Exception("handleIQFeedBars(): Error while processing line: " + StringUtils.join(currentLine, ',') + " :" + e, e), config.getLookupStatsService(), prefix);
        }
    }

    /**
     * This method does the heavy lifting for the get seconds bar request. We requested ticks from IQFeed
     * and we convert them into second granularity bars in this method.
     * The ticks were requested using an HTT lookup request.
     * @param list
     */
    @SuppressWarnings({"ThrowableInstanceNeverThrown"})
    private void handleTicksToSecondBars(ArrayList<String[]> list) {
        // FIXME This should be factored out as a part of a pluggable add-on
        if (list.size() == 0) {
            return; // nothing to do and don't know how to tell any pending call
        }
        String[] first = list.get(0);
        String tag = first[0];

        // Sending command: HTT,BP,20100617 155900,20100617 160000,,093000,160000,1,BP last minute
        // Typical response:
        //   BP last minute,2010-06-17 15:59:00,31.6800,100,109806761,31.6800,31.6900,2850178,0,0,C,
        //   request id,timestamp,price,qty,volume,bid,ask,tickID,bid size,ask size,transaction basis
        // If the trade is in extended hours you get something like:
        //   BP last minute,2010-06-17 16:00:00,31.6900,700,110697543,31.7000,31.7100,2883649,0,0,E,
        //   (note that the last field is E)
        // there are no values defined at this time other than C and E
        // For safety we will only use C values, assuming extended hours are not interesting to us.

        AsyncFunctionFutureResults<List<TimeSlotBarWithTimeStamp>> pending = getBarsRequestPendingCalls();
        // Check for error message
        String lookup1secBars = "lookup_1sec_bars";
        if (first[1].equals("E")) {
            String literalError = first[2];
            if (literalError.equals(IQFEED_NO_DATA_RESPONSE)) {
                // make an empty response
                pending.returnToCallers(tag, new ArrayList<TimeSlotBarWithTimeStamp>(), config.getLookupStatsService(), lookup1secBars);
                config.getAlertService().sendAlert("IQFeed no data response for " + tag);
                return; // and there's nothing else to do here
            }

            boolean canRetry = canRetry(literalError);

            String message = "Error received in response to tick request tagged: '" +
                    tag + "'  literal error: '" + literalError + "'";
            pending.throwException(tag, new IQFeedError(literalError, message, canRetry), config.getLookupStatsService(), lookup1secBars);
            config.getAlertService().sendAlert("IQFeed exception " + message + " (can retry: " + canRetry + ")");
            return;
        }

        String[] currentLine = first;
        String currentSecondString = first[1];
        try {
            ArrayList<TimeSlotBarWithTimeStamp> retval = new ArrayList<TimeSlotBarWithTimeStamp>();  // TimeSlotBar, actually
            TimeSlotBarWithTimeStamp currentBar = new TimeSlotBarWithTimeStamp();

            for (String[] line : list) {
                currentLine = line;

                if (line[1].equals("E")) {
                    pending.throwException(tag, new Exception("Error received in response to bar request tagged: '" +
                            tag + "'  literal error: '" + line[2] + "'"));
                    return;
                }

                String tradeType = line[10];
                if (tradeType.equals("C")) {
                    // exclude extended trading transactions
                    String timestamp = line[1];
                    long timestampMillis = TimeHelper.parse(timestamp).getTimeInMillis();

                    Float price = Float.parseFloat(line[2]);
                    int qty = Integer.parseInt(line[3]);

                    HeavyTickStreamRecord tick = new HeavyTickStreamRecord();
                    tick.setInsertionTimestamp();
                    tick.init(timestampMillis, "unknown symbol", price, qty, null, null); // all the ticks in this case are for the same symbol so it does not really matter what it is
                    if (!timestamp.equals(currentSecondString)) {
                        // we just passed the second boundary - time to close the bar and add it to the list and then start a new bar
                        currentBar.close();
                        Calendar barTimestamp = TimeHelper.parse(currentSecondString);
                        //barTimestamp.add(Calendar.SECOND,-1); // IQFeed gives as timestamp the closing time
                        currentBar.setTimestamp(barTimestamp);
                        retval.add(currentBar);
                        currentBar = new TimeSlotBarWithTimeStamp();
                    }

                    currentBar.add(tick);
                    currentSecondString = timestamp;
                }
            }
            // close and add the last bar
            currentBar.close();
            currentBar.setTimestamp(TimeHelper.parse(currentSecondString));
            retval.add(currentBar);

            pending.returnToCallers(tag, retval, config.getLookupStatsService(), lookup1secBars);
        } catch (Exception e) {
            pending.throwException(tag, new Exception("handleTicksToSecondBars(): Error while processing line: " + StringUtils.join(currentLine, ',') + " :" + e, e), config.getLookupStatsService(), lookup1secBars);
        }
    }

    private boolean canRetry(String literalError) {
        return literalError.contains("Could not connect to History socket") ||
                literalError.contains("Unknown Server Error") ||
                literalError.contains("Socket Error: 10054 (WSAECONNRESET)");  // connection rest by peer
    }

    private int toCents(float price) {
            return Math.round(price*100.0f);
        }

    //</editor-fold>

    // ==============================================================================================================
    // Private methods
    // ==============================================================================================================

    /**
     * Send an alert using the alert service for the parser service bundle
     * @param message
     * @param messageType
     */
    private void sendLookupAlert(String message, String messageType) {
        lookupThreadServiceBundle.getAlertService().sendAlert("IQFeed client (lookup thread): " + message, messageType);
        if (debugFlag) {
            //System.err.println("Alert: "+message);
        }
    }

    private void sampleMemory(long indicator) {
        try {
            long now = System.currentTimeMillis();
            if (now - lastMemStatsSampleTs >= MEMORY_SAMPLE_MIN_INTERVAL) {
                lastMemStatsSampleTs = now;
                getMemStats().sample(indicator);
            }
        } catch (Exception e) {
            System.err.println("Exception: " + e);
            e.printStackTrace(System.err);
        }
    }

    private MemoryStateHistory getMemStats() {
        if (memStats == null) {
            memStats = new MemoryStateHistory(MEMSTATS_BUFFER_SIZE);
        }
        return memStats;
    }

    private void startMessageParserDaemon() {
        Thread parserDaemon = new RealTimeProviderConnectionThread<IDtnIQFeedConfig>(new Runnable() {
            public void run() {
                //                debug("message parse daemon","starting");
                boolean interrupted = false;
                while (daemonsShouldBeRunning.get() && !interrupted) {
                    try {
                        int lines = serviceRawMessageQueue();
                    } catch (InterruptedException e) {
                        interrupted = true;
                        parserDaemonServiceBundle.getAlertService().sendAlert("IQFeed raw message parser daemon INTERRUPTED - exiting thread!");
                    } catch (Throwable t) {
                        sendParserAlert("Exception while processing data from IQConnect: " + t, t);
                        onException(t, false);
                    }
                }
            }
        }, "IQFeed raw message parser daemon: " + debugTag, config);
        parserDaemon.start();
    }

    private void sendParserAlert(String message, Throwable t) {
        sendParserAlert(message, "IQFeed message parser message", t);
    }

    /**
     * Send an alert using the alert service for the parser service bundle
     * @param message
     * @param messageType
     */
    private void sendParserAlert(String message, String messageType, Throwable t) {
        if (t == null) {
            parserDaemonServiceBundle.getAlertService().sendAlert("IQFeed client (parser thread): " + message, messageType);
        } else {
            parserDaemonServiceBundle.getAlertService().sendAlert("IQFeed client (parser thread): " + message, messageType, t);
        }
        if (debugFlag) {
            //System.err.println("Alert: "+message);
        }
    }

    private void parserDaemonIncStat(String s) {
        parserDaemonServiceBundle.getStatsService().incStat(s);
    }

    private void initFacade(IDtnIQFeedConfig config, Map myConfig) throws InvalidArgumentException, InvalidStateException {
        if (myConfig.containsKey("IDtnIQFeedFacade")) {
            // use the instance provided
            facade = (IDtnIQFeedFacade) myConfig.get("IDtnIQFeedFacade");
        } else {
            // try to construct an instance using IDtnIQFeedFacadeImplementationClassName
            String facadeClassName = (String) myConfig.get("IDtnIQFeedFacadeImplementationClassName");    // FIXME refactor dependency injection
            try {
                facade = (IDtnIQFeedFacade) (Class.forName(facadeClassName).newInstance());
            } catch (Exception e) {
                throw new InvalidArgumentException("Failed to create an instance of facade class " + facadeClassName + " :" + e, e);
            }
        }

        if (facade == null) {
            throw new InvalidArgumentException("Could not construct a IDtnIQFeedFacade. Need a valid entry either in 'IDtnIQFeedFacade' (an instance) or in 'IDtnIQFeedFacadeImplementationClassName' (a bean class name implementing IDtnIQFeedFacade)");
        }

        facade.setClient(this);
        facade.init(config);
        initFacadeConnection();
    }

    /**
     * waits for confirmation that facade connected to IQConnect and established a connection for the feed
     *
     * @throws com.moscona.exceptions.InvalidStateException
     */
    private void initFacadeConnection() throws InvalidStateException {
        SafeRunnable runnable = new SafeRunnable() {
            @Override
            public void runWithPossibleExceptions() throws Exception {
                if (config.simpleComponentConfigEntryWithOverride("DtnIQFeedClient", "mode", "dtnIQFeedClientMode").equals("passive")) {
                    config.getAlertService().sendAlert("STARTING IN PASSIVE MODE");
                    facade.startIQConnectPassive();
                } else {
                    facade.startIQConnect();
                }
            }
        };
        new RealTimeProviderConnectionThread(runnable, "IQConnect connection initialization", config).start();

        facadeInitializationTimeoutLock.lock();
        try {
            connectionConfirmedCondition.await(initialConnectionTimeoutMillis, TimeUnit.MILLISECONDS); // return value does not matter. We'll double check anyway
        } catch (InterruptedException e) {
            throw new InvalidStateException("Interrupted while waiting for IQConnect connection confirmation", e);
        } finally {
            facadeInitializationTimeoutLock.unlock();
        }

        if (!connectionConfirmed.get()) {
            throw new InvalidStateException("Timed out while waiting for connection confirmation from the IQFeed facade");
        }
    }

    //<editor-fold desc="Raw message handling">
    /**
     * gets messages off of the raw message queue, parses them, and invokes the appropriate callbacks to act on them
     * @throws InterruptedException
     */
    private int serviceRawMessageQueue() throws InterruptedException {
        MessageQueueItem<String> queueItem = rawMessageQueue.take();
        String rawMessage = queueItem.getPayload();
        long queueLatency = queueItem.getLatency();

        int retval = 0;

        if (hadFatalError) {
            sendParserAlert("Had a fatal error, awaiting restart...", "CRITICAL", null);
        }

        //        debug("pulled message from queue", rawMessage);
        rawMessageQueueSize.decrementAndGet();
        String[] messages = rawMessage.split("\n");
        retval = messages.length;

        for (String message : messages) {
            // first filter out any non-trade quote message (these are the majority, and there is no need to split them)
            if (message.startsWith("Q,") && !message.endsWith("t,,")) {
                //nonTradeTickCounter.incrementAndGet();
                continue;
            }

            // for the rest of the messages - handle with more detail
            String[] fields = message.split(",");

            try {
                switch (fields[0].toCharArray()[0]) {
                    case 'S':
                        onSystemMessage(fields);
                        break;
                    case 'Q':
                        if (message.endsWith("t,,")) {
                            //onTradeQuoteMessage(fields);
                        } else {
                            //onNonTradeQuoteMessage(fields); // just in case we reach this my mistake or decide to handle bid/ask updates
                        }
                        break;
                    case 'F':
                        onFundamentalMessage(fields);
                        break;
                    case 'T':
                        //onTimeStampMessage(fields);
                        break;
                }
            } catch (ArrayIndexOutOfBoundsException e) {
                if (!wasRestartRequested) {
                    sendParserAlert("CORRUPT MESSAGE!!!! MUST RESET IQCONNECT!!!" + e, "CRITICAL", null);
                    sendParserAlert("Raw message: " + rawMessage, "CRITICAL", null);
                    rawMessageQueueSize.set(0);
                    rawMessageQueue.clear();
                    hadFatalError = true;
                    wasRestartRequested = true;
                    config.getEventPublisher().publish(EventPublisher.Event.FATAL_ERROR_RESTART);
                    //                try {
                    //                    facade.doEmergencyRestart();
                    //                }
                    //                catch (InvalidStateException e) {
                    //                    sendParserAlert("FAILED TO RESTARTE IQCONNECT!! "+e,"CRITICAL",e);
                    //                }
                }
                return retval;
            }
        }
        return retval;
    }

    /**
     * Called when the server sends us fundamentals
     *
     * @param fundamentals
     */
    public void onFundamentalMessage(String[] fundamentals) {
        if (!fundamentalsRequested.get()) {
            return; //Don't bother to parse before there was a request for fundamentals data
        }

        if (fundamentalFieldPositions.size() == 0) {
            sendParserAlert("Warning: Fundamentals message received before getting 'S,FUNDAMENTAL FIELDNAMES' - ignoring", null);
            return;
        }

        final String[] stringFields = {"Exchange ID", "Company Name", "Root Option Symbol", "Leaps", "Market Center",
                "Split Factor 1", "Split Factor 2"};
        final String[] intFields = {"NAICS", "Common Shares Outstanding", "Format Code", "Precision",
                "SIC", "Security Type", "Listed Market", "Fiscal Year End"};
        final String[] longFields = {"Average Volume"};
        final String[] floatFields = {"PE", "52 Week High", "52 Week Low",
                "Calendar Year High", "Calendar Year Low", "Dividend Yield", "Dividend Amount", "Dividend Rate",
                "Short Interest", "Current Year EPS", "Next Year EPS", "Five-year Growth Percentage",
                "Percent Held By Institutions", "Beta", "Current Assets", "Current Liabilities",
                "Historical Volatility", "Strike Proce", "Coupon Rate", "Long-term Debt", "Year End Close"};
        final String[] dateFields = {"Pay Date", "Ex-dividend Date", "Balance Sheet Date",
                "52 Week High Date", "52 Week Low Date", "Calendar Year High Date", "Calendar Year Low Date",
                "Maturity Date", "Expiration Date",};

        HashMap<String, Object> values = new HashMap<String, Object>();
        String symbol = fundamentals[fundamentalFieldPositions.get("Symbol")];

        values.put("Symbol", symbol);


        for (String key : stringFields) {
            int index = fundamentalFieldPositions.get(key);
            if (index >= fundamentals.length) {
                continue; // the array of actual values could be shorter than the size
            }
            String value = fundamentals[index];
            values.put(key, value);
        }

        for (String key : intFields) {
            try {
                int index = fundamentalFieldPositions.get(key);
                if (index >= fundamentals.length) {
                    continue; // the array of actual values could be shorter than the size
                }
                String value = fundamentals[index];
                if (!StringUtils.isBlank(value)) {
                    values.put(key, Integer.parseInt(value));
                }
            } catch (Throwable e) {
                sendParserAlert("Error while parsing integer field '" + key + "' in fundamentals message for " + symbol +
                        ". Ignoring and going to next on the list. The value was '" +
                        fundamentals[fundamentalFieldPositions.get(key)] + "'" + " : " + e, e);
            }
        }

        for (String key : longFields) {
            try {
                int index = fundamentalFieldPositions.get(key);
                if (index >= fundamentals.length) {
                    continue; // the array of actual values could be shorter than the size
                }
                String value = fundamentals[index];
                if (!StringUtils.isBlank(value)) {
                    values.put(key, Long.parseLong(value));
                }
            } catch (Throwable e) {
                sendParserAlert("Error while parsing long field '" + key + "' in fundamentals message for " + symbol +
                        ". Ignoring and going to next on the list. The value was '" +
                        fundamentals[fundamentalFieldPositions.get(key)] + "'" + " : " + e, e);
            }
        }

        for (String key : floatFields) {
            try {
                int index = fundamentalFieldPositions.get(key);
                if (index >= fundamentals.length) {
                    continue; // the array of actual values could be shorter than the size
                }
                String value = fundamentals[index];
                if (!StringUtils.isBlank(value)) {
                    values.put(key, Float.parseFloat(value));
                }
            } catch (Throwable e) {
                sendParserAlert("Error while parsing float field '" + key + "' in fundamentals message for " + symbol +
                        ". Ignoring and going to next on the list. The value was '" +
                        fundamentals[fundamentalFieldPositions.get(key)] + "'" + " : " + e, e);
            }
        }

        SimpleDateFormat dateParser = new SimpleDateFormat("MM/dd/yyy");
        for (String key : dateFields) {
            try {
                int index = fundamentalFieldPositions.get(key);
                if (index >= fundamentals.length) {
                    continue; // the array of actual values could be shorter than the size
                }
                String value = fundamentals[index];
                if (!StringUtils.isBlank(value)) {
                    values.put(key, dateParser.parse(value));
                }
            } catch (Throwable e) {
                sendParserAlert("Error while parsing date field '" + key + "' in fundamentals message for " + symbol +
                        ". Ignoring and going to next on the list. The value was '" +
                        fundamentals[fundamentalFieldPositions.get(key)] + "'" + " : " + e, e);
            }
        }

        this.fundamentals.put(symbol, values);
        returnPendingAsyncFundamentalsRequestsOn(symbol, values);
    }

    /**
     * Checks to see whether any async call is waiting on this symbol and if so returns the value to it
     * @param symbol - the symbol in question
     * @param values - the raw results for this value
     */
    private void returnPendingAsyncFundamentalsRequestsOn(String symbol, HashMap<String, Object> values) {
        Fundamentals result = new Fundamentals();
        if (result != null) {

        }
        Object shares = values.get("Common Shares Outstanding");
        if (shares != null) {
            // shares are null for an index
            result.setCommonShares((Integer) shares);
        }
        Object yearHigh = values.get("52 Week High");
        Object yearLow = values.get("52 Week Low");
        Object averageVolume = values.get("Average Volume");
        if (yearHigh == null || yearLow == null || averageVolume == null) {
            sendParserAlert("One of the fundamentals parsed values is null: 52 week high = " + yearHigh +
                    ", 52 week low = " + yearLow + ", avg volume = " + averageVolume, "Fundamentals parsing problem", null);
        }
        if (yearHigh != null) {
            result.setYearHigh((Float) yearHigh);
        }
        if (yearLow != null) {
            result.setYearLow((Float) yearLow);
        }
        if (averageVolume != null) {
            result.setAverageVolume((Long) averageVolume);
        }
        String split1 = values.get("Split Factor 1").toString().trim();
        if (StringUtils.isNotBlank(split1)) {
            result.setSplitFactor1(split1);
        }
        String split2 = values.get("Split Factor 2").toString().trim();
        if (StringUtils.isNotBlank(split2)) {
            result.setSplitFactor2(split2);
        }
        getFundamentalsDataPendingCalls().returnToCallers(symbol, result);
    }

    /**
     * Called for all messages starting with "s,..." The field definitions may be found at <a
     * href="http://www.iqfeed.net/dev/api/docs/SystemMessages.cfm" target="_blank">the IQFeed developer site</a>
     *
     * @param fields
     */
    @SuppressWarnings({"OverlyComplexMethod"})
    private void onSystemMessage(String[] fields) {
        //        debug("System message", fields[1]);
        if (fields[1].equals("STATS")) {
            onStatsMessage(fields);
        } else if (fields[1].equals("SERVER DISCONNECTED")) {
            onConnectionLost();
        } else if (fields[1].equals("SERVER CONNECTED")) {
            // ignore
        } else if (fields[1].equals("SERVER RECONNECT FAILED")) {
            onConnectionLost();
        } else if (fields[1].equals("SYMBOL LIMIT REACHED")) {
            sendParserAlert("Received error from IQFeed: SYMBOL LIMIT REACHED", null);
        } else if (fields[1].equals("FUNDAMENTAL FIELDNAMES")) {
            onFundamentalFieldNamesMessage(fields);
        } else if (fields[1].equals("UPDATE FIELDNAMES")) {
            // ignore - this is all the available field names - we're only interested in the CURRENT UPDATE FIELDNAMES message
        } else if (fields[1].equals("CURRENT UPDATE FIELDNAMES")) {
            onQuoteFieldNamesMessage(fields);
        } else if (fields[1].equals("CURRENT LOG LEVELS")) {
            // ignore
        } else if (fields[1].equals("WATCHES")) {
            //onWatchesMessage(fields); // verify that the watch list matches the market tree
        } else if (fields[1].equals("CUST")) {
            //onCustMessage(fields); // verify that the watch list matches the market tree
        } else {
            delegateSystemMessageToFacade(fields);
        }
    }

    /**
     * Sends to the facade any system messages we have not handled here
     * @param fields
     */
    private void delegateSystemMessageToFacade(String[] fields) {
        try {
            facade.onSystemMessage(fields);
        } catch (InvalidStateException e) {
            sendParserAlert("Error while handling message '" + fields[1] + "' in facade: " + e, e);
        }
    }

    /**
     * Called when we receive the field names from the server - for the fundamentals messages
     *
     * @param fieldNames
     */
    public void onFundamentalFieldNamesMessage(String[] fieldNames) {
        fundamentalFieldPositions.clear();

        for (int i = 2; i < fieldNames.length; i++) {
            String field = fieldNames[i];
            if (field != null && !field.equals("(Reserved)") && !field.equals("")) {
                fundamentalFieldPositions.put(field, i - 1);
            }
        }
    }

    /**
     * Called when we receive the field names from the server - for the quote messages
     *
     * @param fieldNames
     */
    public void onQuoteFieldNamesMessage(String[] fieldNames) {
        fieldPositions.clear();
        for (int i = 2; i < fieldNames.length; i++) {
            fieldPositions.put(fieldNames[i], i - 1);
        }
    }


    /**
     * Called when we get a "S,STATS,..." message
     *
     * @param fields
     */
    @SuppressWarnings({"MagicNumber"})
    private void onStatsMessage(String[] fields) {
        String status = fields[12];
        //        debug("Stats status",status);
        if (status.equals("Not Connected") && isConnected.get()) {
            onConnectionLost();
            return;
        }

        if (status.equals("Connected")) {
            parserDaemonIncStat("IQFeed stats messages");
            final int offset = -1;
            maxSymbols = Integer.parseInt(fields[5 + offset]);
            watchedSymbols = Integer.parseInt(fields[6 + offset]);
            currentInternetBandwidthConsumption = Float.parseFloat(fields[17 + offset]);
            currentLocalBandwidthConsumption = Float.parseFloat(fields[20 + offset]);
            iqFeedVersion = fields[14 + offset];

            if (!iqFeedVersion.equals(IQFEED_COMPATIBLE_VERSION)) {
                sendParserAlert("IQFeed version mismatch! Expected " + IQFEED_COMPATIBLE_VERSION + " but received " + iqFeedVersion, null);
            }

            if (!isConnected.get()) {
                onConnectionEstablished();
            }
        }
    }
    //</editor-fold>


    //******************************************************************************************************************

    //<editor-fold desc="simple getters etc.">
    public String getIqFeedCompatibleVersion() {
        return IQFEED_COMPATIBLE_VERSION;
    }

    public int getMaxSymbols() {
        return maxSymbols;
    }

    public int getWatchedSymbols() {
        return watchedSymbols;
    }

    public String getIqFeedVersion() {
        return iqFeedVersion;
    }

    public float getCurrentInternetBandwidthConsumption() {
        return currentInternetBandwidthConsumption;
    }

    public float getCurrentLocalBandwidthConsumption() {
        return currentLocalBandwidthConsumption;
    }

    public IServicesBundle getParserDaemonServiceBundle() {
        return parserDaemonServiceBundle;
    }

    public int getRawMessageQueueSize() {
        return rawMessageQueueSize.get();
    }

    public int positionOf(String fieldName) {
        Integer position = fieldPositions.get(fieldName);
        if (position == null) {
            return -1;
        } else {
            return position;
        }
    }

    public void setDebugFlag(boolean debugFlag) {
        this.debugFlag = debugFlag;
    }

    public Map getFundamentals(String symbol) {
        return fundamentals.get(symbol);
    }

    public IServicesBundle getHeartBeatServicesBundle() throws InvalidStateException {
        if (heartBeatServicesBundle == null) {
            heartBeatServicesBundle = config.createServicesBundle();
        }
        return heartBeatServicesBundle;
    }

    private synchronized AsyncFunctionFutureResults<List<TimeSlotBarWithTimeStamp>> getBarsRequestPendingCalls() {
        if (barsRequestPendingCalls == null) {
            barsRequestPendingCalls = new AsyncFunctionFutureResults<List<TimeSlotBarWithTimeStamp>>();
        }
        return barsRequestPendingCalls;
    }

    private synchronized AsyncFunctionFutureResults<Fundamentals> getFundamentalsDataPendingCalls() {
        if (fundamentalsDataPendingCalls == null) {
            fundamentalsDataPendingCalls = new AsyncFunctionFutureResults<Fundamentals>();
        }
        return fundamentalsDataPendingCalls;
    }

    //</editor-fold>

    // ==============================================================================================================
    // Inner classes
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

        public void setSplitFactor1(String splitFactor) {
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

        public void setSplitFactor2(String splitFactor) {
            this.splitFactor2 = splitFactor;
            if (StringUtils.isBlank(splitFactor)) {
                return;
            }
            splitFactor2factor = extractFactor(splitFactor);
            splitFactor2date = extractDate(splitFactor);
        }

        private String extractDate(String splitFactor) {
            String[] parts = splitFactor.split(" +");
            return parts[1].trim();
        }

        private float extractFactor(String splitFactor) {
            String[] parts = splitFactor.split(" +");
            return Float.parseFloat(parts[0].trim());
        }

        public Pair<Calendar, Float> getLastSplit() throws InvalidArgumentException {
            Calendar date1 = null;
            Float ratio1 = null;
            Calendar date2 = null;
            Float ratio2 = null;
            if (StringUtils.isNotBlank(getSplitFactor1date())) {
                date1 = TimeHelper.parseMmDdYyyyDate(getSplitFactor1date());
                ratio1 = getSplitFactor1factor();
            }
            if (StringUtils.isNotBlank(getSplitFactor2date())) {
                date1 = TimeHelper.parseMmDdYyyyDate(getSplitFactor2date());
                ratio1 = getSplitFactor2factor();
            }
            if (date1 == null && date2 == null) {
                return null;
            }

            if (date1 != null && date2 == null) {
                return new Pair<Calendar, Float>(date1, ratio1);
            }

            if (date1 == null && date2 != null) {
                return new Pair<Calendar, Float>(date2, ratio2);
            }

            // both not null - pick the later one
            if (date1.compareTo(date2) > 0) {
                return new Pair<Calendar, Float>(date1, ratio1);
            } else {
                return new Pair<Calendar, Float>(date2, ratio2);
            }
        }
    }

    public class IQFeedError extends Exception {
        private String literalError;
        private boolean isRetriable = true;

        protected IQFeedError(String literalError, String message, boolean isRetriable) {
            super(message);
            this.literalError = literalError;
            this.isRetriable = isRetriable;
        }

        public String getLiteralError() {
            return literalError;
        }

        public boolean isRetriable() {
            return isRetriable;
        }
    }
}
