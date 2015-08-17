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

import com.moscona.events.EventPublisher;
import com.moscona.threads.RealTimeProviderConnectionThread;
import com.moscona.trading.IServicesBundle;
import com.moscona.trading.adapters.DataSourceCoreStats;
import com.moscona.trading.adapters.iqfeed.lookup.DtnIQFeedMinuteLookup;
import com.moscona.trading.adapters.iqfeed.lookup.ILookupResponseHandler;
import com.moscona.trading.elements.SymbolChart;
import com.moscona.trading.excptions.MissingSymbolException;
import com.moscona.trading.formats.deprecated.MarketTree;
import com.moscona.trading.persistence.SplitsDb;
import com.moscona.trading.streaming.HeavyTickStreamRecord;
import com.moscona.util.ExceptionHelper;
import com.moscona.util.SafeRunnable;
import com.moscona.util.async.AsyncFunctionCall;
import com.moscona.util.async.AsyncFunctionFutureResults;
import com.moscona.exceptions.InvalidArgumentException;
import com.moscona.exceptions.InvalidStateException;
import com.moscona.trading.elements.Bar;
import com.moscona.trading.streaming.TimeSlotBarWithTimeStamp;
import com.moscona.util.TimeHelper;
import com.moscona.util.collections.Pair;
import com.moscona.util.monitoring.MemoryStateHistory;
import com.moscona.util.monitoring.stats.IStatsService;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Arnon on 5/8/2014.
 * Derived from code from another project.
 */
public class DtnIqfeedHistoricalClient implements IDtnIQFeedHistoricalClient {
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
     * Also CompletableFuture should come in handy. The main issuie with it would be that you don't want user code
     * running in the adapter thread
     * ***************************************************************************************************************
     */
      //<editor-fold desc="Constants">

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

    public static final String NAME = "IQFeedHistorical";
    //</editor-fold>

    //<editor-fold desc="private fields">
    private IDtnIQFeedFacade facade = null;
    private boolean initialized = false;
    private boolean isShutDown = false;

    private AtomicBoolean daemonsShouldBeRunning;
    @SuppressWarnings("FieldCanBeLocal")
    private Thread heartBeatThread = null; // guarantees some code gets executed at a regular frequency

    private int initialConnectionTimeoutMillis;
    private final ReentrantLock facadeInitializationTimeoutLock;
    private Condition connectionConfirmedCondition;
    private AtomicBoolean connectionConfirmed; // needed for double-checking
    private AtomicBoolean isConnected;


    private ConcurrentHashMap<String, Integer> fieldPositions;
    private ConcurrentHashMap<String, Map> fundamentals;
    private ConcurrentHashMap<String, Integer> fundamentalFieldPositions;

    private AtomicInteger rawMessageQueueSize;                    // counting so that we don't have to call size() on the queue
    private LinkedBlockingQueue<MessageQueueItem<String>> rawMessageQueue;          // raw data without pre-processing as it came from the facade
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
    // FIXME the following should go away after refactoring is done
    private ConcurrentHashMap<String, Integer> tagToHandlerIdMap = null;  // maps the callback tag for async responses to data handler IDs
    private ConcurrentHashMap<String, ILookupResponseHandler<ArrayList<String[]>>> tagToResponseHandlerMethodMap = null; // maps tags to the specific callable that's going to handle it (instead of to a numeric code that represents the method)
    private ConcurrentHashMap<String, ArrayList<String[]>> pendingLookupData; // holds tagged lookup data that has not been completed yet (no !ENDMSG! seen)

    private AsyncFunctionFutureResults<Fundamentals> fundamentalsDataPendingCalls = null;
    private AtomicBoolean fundamentalsRequested;
    private boolean hadFatalError = false;
    private boolean wasRestartRequested = false;

    private PrintWriter timingLogFile = null;


    private AtomicInteger lastPublishedStatsTimestamp = new AtomicInteger(0);

    private long lastMemStatsSampleTs = 0;
    public static final long MEMORY_SAMPLE_MIN_INTERVAL = 1000; // 1 second
    public static final int MEMSTATS_BUFFER_SIZE = 1200; // 20 min
    private MemoryStateHistory memStats;
    private int maxSymbols;
    private int watchedSymbols;
    private String iqFeedVersion;
    private float currentInternetBandwidthConsumption;
    private float currentLocalBandwidthConsumption;

     //</editor-fold>

    public DtnIqfeedHistoricalClient() {
        daemonsShouldBeRunning = new AtomicBoolean(true);
        facadeInitializationTimeoutLock = new ReentrantLock();
        connectionConfirmedCondition = facadeInitializationTimeoutLock.newCondition();
        connectionConfirmed = new AtomicBoolean(false);
        isConnected = new AtomicBoolean(false);

        rawMessageQueue = new LinkedBlockingQueue<>();
        rawMessageQueueSize = new AtomicInteger(0);

        fieldPositions = new ConcurrentHashMap<>();

        fundamentalFieldPositions = new ConcurrentHashMap<>();
        fundamentals = new ConcurrentHashMap<>();

        blockingWaitCounter = new AtomicInteger(0);

        tagToHandlerIdMap = new ConcurrentHashMap<>(); // FIXME goes away after refactoring done
        tagToResponseHandlerMethodMap = new ConcurrentHashMap<>();
        pendingLookupData = new ConcurrentHashMap<>();
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
            Map myConfig = (Map)config.getComponentConfigFor("DtnIqfeedHistoricalClient");
            defaultServiceBundle = config.getServicesBundle();
            parserDaemonServiceBundle = config.createServicesBundle();
            lookupThreadServiceBundle = config.createServicesBundle();

            initialConnectionTimeoutMillis = (Integer)myConfig.get("connectionTimeoutMillis");
            if (initialConnectionTimeoutMillis < 1) {
                throw new InvalidArgumentException("The connectionTimeoutMillis parameter must be a positive integer");
            }

            startMessageParserDaemon();

            initFacade(config, myConfig);

            heartBeatServicesBundle = config.createServicesBundle();
            startHeartBeat();
            listenForStatsUpdateRequests();

            initialized = true;
        } catch (NumberFormatException e) {
            throw new InvalidArgumentException("error while initializing IQFeed client: "+e, e);
        } catch (ClassCastException e) {
            throw new InvalidArgumentException("error while initializing IQFeed client: "+e, e);
        } catch (Throwable e) {
            throw new InvalidStateException("error while initializing IQFeed client: "+e, e);
        }
    }

//    private void debug(String title, String s, Throwable e) {
//        System.err.println("______________________________________________________________________");
//        System.err.println("DEBUG: "+title+"\n\n");
//        System.err.println(s);
//        if (e!=null) {
//            e.printStackTrace(System.err);
//        }
//        System.err.println("\n\n______________________________________________________________________\n\n");
//    }

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
    private void onHeartBeat() throws InvalidStateException, InvalidArgumentException {
        monitorRawMessageQueueSize(rawMessageQueueSize.get());
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

    private void collectStatsCounters() throws InvalidStateException, InvalidArgumentException {
        final IStatsService stats = getHeartBeatServicesBundle().getStatsService();
        String prefix = "IQFeed client ";
        stats.setStat(prefix + "rawMessageQueueSize", rawMessageQueueSize.get());
        stats.setStat(prefix + "blockingWaitCounter", blockingWaitCounter.get());
        stats.setStat(prefix + "currentLocalBandwidthConsumption", currentLocalBandwidthConsumption);
        stats.setStat(prefix + "currentInternetBandwidthConsumption", currentInternetBandwidthConsumption);
    }

    private void listenForStatsUpdateRequests() {
        config.getEventPublisher().onEvent(EventPublisher.Event.STATS_UPDATE_REQUEST, new SafeRunnable() {
            @Override
            public void runWithPossibleExceptions() throws Exception {
                publishStatsUpdate();
            }
        }.setName("IQFeed stats update request listener"));

//        config.getEventPublisher().onEvent(EventPublisher.Event.DUMP_MEMORY_TRACKER_REQUEST, new SafeRunnable() {
//            @Override
//            public void runWithPossibleExceptions() throws Exception {
//                dumpMemStats();
//            }
//        }.setName("IQFeed DUMP_MEMORY_TRACKER_REQUEST responder"));
    }

//    protected  void dumpMemStats() {
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
//    }


    protected void publishStatsUpdate() throws InvalidStateException, InvalidArgumentException {
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

    public boolean isConnected() {
        return isConnected.get();
    }

    public boolean isShutdown() {
        return isShutDown;
    }

    //</editor-fold>

    @Override
    /**
     * Called whenever the facade encounters an exception.
     *
     * @param ex         the exception
     * @param wasHandled whether or not it was handled already
     */
    public void onException(Throwable ex, boolean wasHandled) {
        //fixme implement DtnIqfeedHistoricalClient.onException(): publish exception via MBassador
    }

    @Override
    /**
     * Called when the facade receives new data from the admin connection. This is typically more than one message.
     *
     * @param data the data as a String
     */
    public void onAdminData(String data) {
        sampleMemory(data.length());
        rawMessageQueue.add(new MessageQueueItem<String>(data));
        //int queueSize = rawMessageQueueSize.incrementAndGet();
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


    /**
     * Registers a response handler for a specific tag. The handler gets called once the entire response is done, but not
     * before. This is for a synchronous wrapper style. A more streaming approach is done separately (if it gets
     * implemented at all)
     * @param tag
     * @param handler
     */
    public void registerResponseHandler(String tag, ILookupResponseHandler<ArrayList<String[]>> handler) {
        tagToResponseHandlerMethodMap.put(tag, handler); // this should auto-deregister once the handler returns
    }

    @Override
    /**
     * A callback for the facade to notify the client about new data received on the lookup port
     *
     * @param rawMessages
     */
    public void onLookupData(String rawMessages) throws InvalidStateException, InvalidArgumentException {
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

            if (! tagToHandlerIdMap.containsKey(tag) && !tagToResponseHandlerMethodMap.containsKey(tag)) {
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
                    list = new ArrayList<>();
                    pendingLookupData.put(tag,list);
                }
                list.add(fields);
            }
        }
    }

    //<editor-fold desc="IHistoricalDataSource methods">

    @Override
    public String getName() throws InvalidStateException {
        return NAME;
    }

    @Override
    /**
     * Given a symbol and a splits DB - queries the data source and updates the splits DB with the latest available
     * information about splits for this symbol
     *
     * @param symbol
     * @param splits
     */
    public void updateSplitsFor(String symbol, SplitsDb splits, int timeout, TimeUnit timeoutUnit) throws InvalidArgumentException, InvalidStateException, TimeoutException, MissingSymbolException {
        // FIXME Splits should be expressed by an interface rather than a concrete class
        Fundamentals fundamentals = getFundamentalsDataFor(symbol, timeout, timeoutUnit);
        if (fundamentals.hasSplitFactor1()) {
            // IMPORTANT: IQFeed gives only 2 digit ratios. This requires aggressive correction of the common 1/3 and 2/3 multiples
            double ratio = fixSplitRatio(fixSplitRatio(fundamentals.getSplitFactor1factor(), 3, 0.11f), 100, 0.0001f);
            splits.add(fundamentals.getSplitFactor1date(), symbol, ratio, null, ratio > 0.0);
        }
        if (fundamentals.hasSplitFactor2()) {
            // IMPORTANT: IQFeed gives only 2 digit ratios. This requires aggressive correction of the common 1/3 and 2/3 multiples
            double ratio = fixSplitRatio(fixSplitRatio(fundamentals.getSplitFactor2factor(), 3, 0.11f), 100, 0.0001f);
            splits.add(fundamentals.getSplitFactor2date(), symbol, ratio, null, ratio > 0.0);
        }
    }

    private double fixSplitRatio(double ratio, int multiple, float roundingError) {
        double trippleRatio = multiple * ratio;
        if (Math.abs(Math.round(trippleRatio) - trippleRatio) < roundingError) {
            return ((double) Math.round(trippleRatio)) / multiple;
        }
        return ratio;
    }

    /**
     * Gets the fundamental data for a specific symbol.
     * This is done somewhat similar to the lookup interface (sync call over an async medium) but the big difference is
     * that the fundamentals are transmitted over the level1 streaming data, and not always when we request them.
     * Also, there is no request/response tagging mechanism in the protocol for the level1 streaming data, whereas
     * in the lookup port everything can be tagged ans so we can have a close to generic handling of call/response in
     * the lookup side while this requires a somewhat different handling, unfortunately.
     * @param symbol
     * @param timeout
     * @param unit
     * @return
     * @throws com.moscona.exceptions.InvalidStateException
     */
    private Fundamentals getFundamentalsDataFor(String symbol, int timeout, TimeUnit unit) throws InvalidStateException {
        // FIXME should consider making this a public (interface) method
        AsyncFunctionFutureResults<Fundamentals> pending = getFundamentalsDataPendingCalls();
        AsyncFunctionCall<Fundamentals> fundamentalsCall = new FundamentalsDataFacadeCall(symbol).use(pending);

        try {
            Fundamentals retval = fundamentalsCall.call(timeout, unit);
            return retval;
        } catch (Exception e) {
            throw new InvalidStateException("Exception while trying to get fundamentals data for " + symbol, e);
        }
    }

    @Override
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
     * @throws com.intellitrade.exceptions.MissingSymbolException
     *
     */
    public SymbolChart getMinuteBars(String symbol, Calendar from, Calendar to, int timeout, TimeUnit unit, int retryLimit) throws InvalidArgumentException, InvalidStateException, TimeoutException, MissingSymbolException {
        return new DtnIQFeedMinuteLookup(this).getMinuteBars(symbol, from, to, timeout, unit, retryLimit);
    }

    @Override
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
     * @throws com.intellitrade.exceptions.MissingSymbolException
     *
     */
    public SymbolChart getSecondBars(String symbol, Calendar from, Calendar to, int timeout, TimeUnit unit, int retryLimit) throws InvalidArgumentException, InvalidStateException, TimeoutException, MissingSymbolException {
        // The code for this method is almost identical to getMinuteBars() - the main difference is in the handler
        int requestStarted = TimeHelper.now();

        String startString = toIqFeedTimestamp(from);
        Calendar requestTo = (Calendar) to.clone();
        requestTo.add(Calendar.SECOND, -1);
        String endString = toIqFeedTimestamp(requestTo);

        AsyncFunctionFutureResults<List<TimeSlotBarWithTimeStamp>> pending = getBarsRequestPendingCalls();
        List<TimeSlotBarWithTimeStamp> answer = null;
        int remainingAttempts = retryLimit + 1;

        do {
            remainingAttempts--;
            String tag = "getSecondBars(" + symbol + ") " + new Date().getTime();
            try {
                tagToHandlerIdMap.put(tag, HANDLER_TICKS_TO_SECOND_BARS); // control data routing in the onLookupData method
                AsyncFunctionCall<List<TimeSlotBarWithTimeStamp>> secondsBarCall = new SecondsBarCall(symbol, startString, endString, tag).use(pending);
                answer = secondsBarCall.call(timeout, unit);
                logSymbolTiming(symbol, requestStarted, "getSecondBars/secondsBarCall", null);
            } catch (IQFeedError e) {
                handleLookupIQFeedError(e, symbol, requestStarted, "getSecondBars/secondsBarCall", "IQFeedError while trying to get second data for " + symbol + ": " + e);
            } catch (Exception e) {
                Throwable iqFeedError = ExceptionHelper.fishOutOf(e, IQFeedError.class, 20);
                if (iqFeedError != null) {
                    handleLookupIQFeedError((IQFeedError) iqFeedError, symbol, requestStarted, "getSecondBars/secondsBarCall", "IQFeedError while trying to get second data for " + symbol + ": " + e);
                } else {
                    logSymbolTiming(symbol, requestStarted, "getSecondBars/secondsBarCall", e);
                    throw new InvalidStateException("Exception while trying to get second data for " + symbol + " with timeout " + timeout + " " + unit + ": " + e, e);
                }
            } finally {
                tagToHandlerIdMap.remove(tag); // this tag is unique(ish) and is for one time use
            }
        } while (remainingAttempts > 0 && answer == null);

        // verify the return value
        long expectedCount = 1 + (to.getTimeInMillis() - from.getTimeInMillis()) / 1000;
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
            InvalidStateException e = new InvalidStateException("getSecondBars(): Got an answer with the wrong number of bars. Expected " + expectedCount +
                    " but got " + answer.size() + " bars instead. " +
                    "the first bar is " + firstBar + " and the last bar is " + lastBar);
            logSymbolTiming(symbol, requestStarted, "getSecondBars", e);
            throw e;
        }

        // Convert the answer into a SymbolChart
        int oneSecond = 1000;
        SymbolChart retval = new SymbolChart(symbol,
                TimeHelper.timeStampRelativeToMidnight(from),
                TimeHelper.timeStampRelativeToMidnight(to), oneSecond);

        int remaining = retval.capacity();
        fillChartFromBars(answer, retval, 0);


        return retval;
    }


    //</editor-fold>

    //<editor-fold desc="market tree data">

    // FIXME the whole set of market tree data methods should go away and be replaced by daily bars methods
    /**
     * Gets the a daily quote (close time) at the date passed for a symbol with all the information required for the
     * market tree.
     * IMPORTANT: this data comes as-is from IQFeed, which is split-adjusted to the present.
     * IMPORTANT: when this method fetches data for the past (older than yesterday) it does *not* pull fundamentals, therefore *not* giving data that comed from the IQFeed fundamentals
     *
     * @param date the date for which the market tree build is needed
     * @param symbol the symbol the query is for
     * @param timeout
     * @param unit
     * @return a MarketTreeBar (specialized version of Bar)
     * @throws com.moscona.exceptions.InvalidArgumentException
     *          if the symbol does not exist or is not quotable
     * @throws com.moscona.exceptions.InvalidStateException
     *          if there were other problems
     * @throws java.util.concurrent.TimeoutException
     */
    public MarketTree.TreeEntry getMarketTreeDataFor(Calendar date, String symbol, int timeout, TimeUnit unit) throws InvalidArgumentException, InvalidStateException, TimeoutException {
        int retryLimit = 3;
        Calendar yesterday = (Calendar) TimeHelper.nowAsCalendar().clone();
        yesterday.add(Calendar.HOUR, -24);
        if (TimeHelper.isSameDay(date) || TimeHelper.isSameDay(date, yesterday)) {
            // this is the "old" method - getting data from fundamentals
            return getMarketTreeDataForToday(symbol, timeout, unit);
        }

        Calendar oneYearBefore = (Calendar) date.clone();
        oneYearBefore.add(Calendar.YEAR, -1);
        String from = toIqFeedDate(oneYearBefore);
        String to = toIqFeedDate(date);

        AsyncFunctionFutureResults<List<TimeSlotBarWithTimeStamp>> pending = getBarsRequestPendingCalls();
        int remainingAttempts = retryLimit + 1;
        List<TimeSlotBarWithTimeStamp> answer = null;

        do {
            remainingAttempts--;
            String tag = "getDayBars(" + symbol + ") " + from + "-" + to + " " + new Date().getTime();
            tagToHandlerIdMap.put(tag, HANDLER_DAY_BARS); // control data routing in the onLookupData method

            AsyncFunctionCall<List<TimeSlotBarWithTimeStamp>> dayBarCall = new DayBarCall(symbol, from, to, tag).use(pending);
            answer = null;
            int requestStarted = TimeHelper.now();

            try {
                answer = dayBarCall.call(timeout, unit);
            } catch (IQFeedError e) {
                boolean shouldRetry = e.isRetriable() && remainingAttempts > 0;
                if (!shouldRetry) {
                    throw new InvalidStateException("IQFeedError while trying to get one year of day data for " + symbol + ": " + e, e);
                } else {
                    config.getAlertService().sendAlert("Retrying after IQFeedError while trying to get one year of day data for " + symbol + ": " + e + " (remaining attempts: " + remainingAttempts + ")");
                }
            } catch (Exception e) {
                Throwable iqFeedError = ExceptionHelper.fishOutOf(e, IQFeedError.class, 20);
                if (iqFeedError != null) {
                    throw new InvalidStateException("IQFeedError while trying to get one year of day data for " + symbol + ": " + iqFeedError, iqFeedError);
                } else {
                    throw new InvalidStateException("Exception while trying to get one year of day data for " + symbol + " with timeout " + timeout + " " + unit + ": " + e, e);
                }
            } finally {
                tagToHandlerIdMap.remove(tag); // this tag is unique(ish) and is for one time use
            }
        } while (remainingAttempts > 0 && answer == null);

        // don't know how many we get. Some symbols are new and don't have a lot of history
        if (answer.size() < 1) {
            throw new InvalidStateException("Got too few responses for one year of day bars for " + symbol + ": " + answer.size());
        }

        TimeSlotBarWithTimeStamp lastResponse = answer.get(answer.size() - 1);

        // verify that first and last are within 5 days from requested boundaries
        // IMPORTANT: Again because symbols can start or stop trading anywhere in the year - this check does not make sense
        //        String errStr = "response to a one year request is too far away from the requested date of ";
        //        if (Math.abs(TimeHelper.daysDiff(oneYearBefore, answer.get(0).getTimestamp())) > 5) {
        //            throw new InvalidStateException("The first " + errStr + TimeHelper.toDayStringMmDdYyyy(oneYearBefore));
        //        }
        //        if(Math.abs(TimeHelper.daysDiff(date, lastResponse.getTimestamp())) > 5) {
        //            throw new InvalidStateException("The last " + errStr + TimeHelper.toDayStringMmDdYyyy(date));
        //        }

        // calculate 52 week high, 52 week low
        float yearHigh = lastResponse.getClose();
        float yearLow = lastResponse.getClose();
        for (TimeSlotBarWithTimeStamp bar : answer) {
            if (TimeHelper.isDayWithinRange(bar.getTimestamp(), oneYearBefore, date)) {
                float close = bar.getClose();
                yearHigh = Math.max(yearHigh, close);
                yearLow = Math.min(yearLow, close);
            }
        }

        // calculate average daily volume going one month back
        long cumulativeVolume = 0;
        int days = 0;
        Calendar oneMontheBefore = (Calendar) date.clone();
        oneMontheBefore.add(Calendar.MONTH, -1);
        for (TimeSlotBarWithTimeStamp bar : answer) {
            if (TimeHelper.isDayWithinRange(bar.getTimestamp(), oneMontheBefore, date)) {
                cumulativeVolume += bar.getVolume();
                days++;
            }
        }
        long dailyAverageVolume = (days > 0) ? cumulativeVolume / days : 0;

        // calculate close
        // create a market tree entry
        MarketTree.TreeEntry entry = new MarketTree().newTreeEntry();
        entry.setName(symbol);
        entry.setAverageDailyVolume(dailyAverageVolume);
        entry.setClass1("value");
        entry.setPrevClose(lastResponse.getClose());
        entry.setYearHigh(yearHigh);
        entry.setYearLow(yearLow);

        // return it
        return entry;
    }

    /**
     * Gets the latest available daily quote (close time) for a symbol with all the information required for the
     * market tree.
     *
     * @param symbol the symbol the query is for
     * @param timeout
     * @param unit
     * @return a MarketTreeBar (specialized version of Bar)
     * @throws com.moscona.exceptions.InvalidArgumentException
     *          if the symbol does not exist or is not quotable
     * @throws com.moscona.exceptions.InvalidStateException
     *          if there were other problems
     */
    public MarketTree.TreeEntry getMarketTreeDataFor(String symbol, int timeout, TimeUnit unit) throws InvalidArgumentException, InvalidStateException, TimeoutException {
        return getMarketTreeDataForToday(symbol, timeout, unit);
    }

    public MarketTree.TreeEntry getMarketTreeDataForToday(String symbol, int timeout, TimeUnit unit) throws InvalidArgumentException, InvalidStateException, TimeoutException {
        int requestStarted = TimeHelper.now();
        MarketTree.TreeEntry entry = new MarketTree().newTreeEntry(); // changed in response to weird compiler error in TeamCity
        long started = System.currentTimeMillis();

        // get last daily close data
        Bar bar = getLastCloseWithThirtyDayAvgVolume(symbol, timeout, unit);
        logSymbolTiming(symbol, requestStarted, "getMarketTreeDataFor/getLastCloseWithThirtyDayAvgVolume", null);

        long remaining = unit.toMillis(timeout) - (System.currentTimeMillis() - started);
        if (remaining <= 0) {
            throw new TimeoutException("Timeout while getting market tree data for " + symbol);
        }
        // get fundamentals data
        Fundamentals fundamentals = getFundamentalsDataFor(symbol, (int) remaining, TimeUnit.MILLISECONDS);
        logSymbolTiming(symbol, requestStarted, "getMarketTreeDataFor/getFundamentalsDataFor", null);

        // fill out row and return
        entry.setPrevClose(bar.getClose());
        entry.setYearHigh(fundamentals.getYearHigh());
        entry.setYearLow(fundamentals.getYearLow());
        entry.setName(symbol);
        // the actual entry we have is not from the market tree and its type has not been determined
        // moreover, in this context it may not yet be in the market tree at all.
        int entryType = config.getMasterTree().get(symbol).getType();
        if (entryType == MarketTree.TreeEntry.STOCK) {
            // the following do not apply for INDEX nodes
            entry.setAverageDailyVolume(fundamentals.getAverageVolume());
            entry.setMarketCap(fundamentals.getCommonShares() * bar.getClose());
            entry.setShares(fundamentals.getCommonShares());
        }

        // find the last split info
        Pair<Calendar, Float> lastSplit = fundamentals.getLastSplit();
        if (lastSplit != null) {
            HashMap<String, Object> decorations = entry.getDecorations();
            decorations.put("lastSplitDate", lastSplit.getFirst());       // Related to call to com.intellitrade.server.tasks.DailyActivities.fixStockCounts()
            decorations.put("lastSplitRatio", lastSplit.getSecond());
        }
        return entry;
    }

    private Bar getLastCloseWithThirtyDayAvgVolume(String symbol, long timeout, TimeUnit unit) throws InvalidStateException {
        AsyncFunctionFutureResults<Bar> pending = getDailyDataPendingCalls();
        String tag = TAG_PREFIX_31_DAY + symbol;
        tagToHandlerIdMap.put(tag, HANDLER_31_DAY_BARS); // control data routing in the onLookupData method
        AsyncFunctionCall<Bar> dailyDataCall = new DailyDataFacadeCall(symbol, tag).use(pending);
        try {
            Bar retval = dailyDataCall.call(timeout, unit);
            return retval;
        } catch (Exception e) {
            throw new InvalidStateException("Exception while trying to get daily data for " + symbol + " with timeout " + timeout + " " + unit, e);
        }
    }

    private synchronized AsyncFunctionFutureResults<Bar> getDailyDataPendingCalls() {
        if (dailyDataPendingCalls == null) {
            dailyDataPendingCalls = new AsyncFunctionFutureResults<Bar>();
        }
        return dailyDataPendingCalls;
    }



    //</editor-fold>


    // ==============================================================================================================
    // Lookup handling
    // ==============================================================================================================

    //<editor-fold desc="Lookup handling">

    private void removeFromPendingAndHandle(String tag, ArrayList<String[]> list) throws InvalidStateException, InvalidArgumentException {
        long start = System.currentTimeMillis();
        getBarsRequestPendingCalls().markLastByteTimeStamp(tag);
        getBarsRequestPendingCalls().setDataSize(tag, list.size());
        try {
            pendingLookupData.remove(tag);
            if (tagToResponseHandlerMethodMap.containsKey(tag)) {
                // give priority to the registered handler
                tagToResponseHandlerMethodMap.get(tag).handleResponse(list);
            }
            else {
                //FIXME this needs to be cleaned up for packaged handlers. No predefined IDs
                int handlerId = tagToHandlerIdMap.get(tag);
                switch (handlerId) {
                    case HANDLER_31_DAY_BARS:
                        handle31DayBars(list);
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
            }
        } finally {
            long processingTime = System.currentTimeMillis() - start;
            getBarsRequestPendingCalls().setPostResponseProcessingTime(tag, processingTime);
            if (tagToResponseHandlerMethodMap.containsKey(tag)) {
                tagToResponseHandlerMethodMap.remove(tag);
            }
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
    private void handleTicksToSecondBars(ArrayList<String[]> list) throws InvalidStateException, InvalidArgumentException {
        // FIXME This should be factored out as a part of a pluggable add-on
        // FIXME With IQFeed 5.x should be able to directly lookup seconds without having to lookup ticks
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

    public boolean canRetry(String literalError) {
        return literalError.contains("Could not connect to History socket") ||
                literalError.contains("Unknown Server Error") ||
                literalError.contains("Socket Error: 10054 (WSAECONNRESET)");  // connection rest by peer
    }

    //FIXME probably should be removed after refactoring
    private int toCents(float price) {
        return Math.round(price * 100.0f);
    }

    /**
     * Helper function to factor common code of potentially retriable errors
     *
     * @param e
     * @param symbol
     * @param requestStarted
     * @param logTag
     * @param msg
     * @throws InvalidStateException
     */
    public void handleLookupIQFeedError(IQFeedError e, String symbol, int requestStarted, String logTag, String msg) throws InvalidStateException {
        logSymbolTiming(symbol, requestStarted, logTag, e);
        if (!e.isRetriable()) {
            throw new InvalidStateException(msg, e);
        }
    }

    //</editor-fold>

    // ==============================================================================================================
    // Private methods
    // ==============================================================================================================

    //<editor-fold desc="misc private methods">
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

    public void logSymbolTiming(String symbol, int requestStarted, String context, Exception e) {
        try {
            synchronized (this) {
                if (timingLogFile == null) {
                    String logFileLocation = config.getStreamDataStoreRoot() + "/symbolTimingLog.txt";  // FIXME replace with proper injector of a proper logger
                    FileUtils.touch(new File(logFileLocation));
                    //noinspection IOResourceOpenedButNotSafelyClosed
                    timingLogFile = new PrintWriter(new FileOutputStream(logFileLocation, true), true);
                }
            }
            StringBuilder msg = new StringBuilder();
            String delimiter = ";";
            msg.append(symbol).append(delimiter);
            msg.append(context).append(delimiter);
            msg.append(TimeHelper.now() - requestStarted).append(delimiter);
            if (e == null) {
                msg.append("OK");
            } else {
                String filename = "";
                int lineNo = -1;
                String methodName = "";
                StackTraceElement[] stackTrace = e.getStackTrace();
                if (stackTrace != null && stackTrace.length > 0 && stackTrace[0] != null) {
                    filename = stackTrace[0].getFileName();
                    lineNo = stackTrace[0].getLineNumber();
                    methodName = stackTrace[0].getMethodName();
                }
                msg.append(e.getClass().getName()).append(delimiter).append(filename).append(delimiter)
                        .append(lineNo).append(delimiter).append(methodName).append("();")
                        .append(e.toString().replaceAll(delimiter, ":"));
            }
            synchronized (this) {
                timingLogFile.println(msg);
            }
        } catch (Exception e1) {
            sendLookupAlert("Error while logging timing: " + e1, "Logging error");
        }
    }

    /**
     * Converts a calendar to an IQFeed timestamp "yyyyMMdd hhmmss"
     * @param cal
     * @return
     */
    public String toIqFeedTimestamp(Calendar cal) { //FIXME I think this should really be in the facade, and that the facade should be able to consume Calendar objects directly, as well as the appropriate Java8 date/time objects
        StringBuilder retval = new StringBuilder();

        retval.append(cal.get(Calendar.YEAR));
        retval.append(StringUtils.leftPad(Integer.toString(cal.get(Calendar.MONTH) + 1), 2, '0'));
        retval.append(StringUtils.leftPad(Integer.toString(cal.get(Calendar.DAY_OF_MONTH)), 2, '0'));
        retval.append(" ");
        retval.append(StringUtils.leftPad(Integer.toString(cal.get(Calendar.HOUR_OF_DAY)), 2, '0'));
        retval.append(StringUtils.leftPad(Integer.toString(cal.get(Calendar.MINUTE)), 2, '0'));
        retval.append(StringUtils.leftPad(Integer.toString(cal.get(Calendar.SECOND)), 2, '0'));

        return retval.toString();
    }

    /**
     * Converts a calendar to an IQFeed date "yyyyMMdd"
     *
     * @param cal
     * @return
     */
    private String toIqFeedDate(Calendar cal) { //FIXME I think this should really be in the facade, and that the facade should be able to consume Calendar objects directly, as well as the appropriate Java8 date/time objects
        StringBuilder retval = new StringBuilder();

        retval.append(cal.get(Calendar.YEAR));
        retval.append(StringUtils.leftPad(Integer.toString(cal.get(Calendar.MONTH) + 1), 2, '0'));
        retval.append(StringUtils.leftPad(Integer.toString(cal.get(Calendar.DAY_OF_MONTH)), 2, '0'));

        return retval.toString();
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
        Thread parserDaemon = new RealTimeProviderConnectionThread<>(() -> {
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
                if (config.simpleComponentConfigEntryWithOverride("DtnIqfeedHistoricalClient", "mode", "dtnIQFeedClientMode").equals("passive")) {
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

    //FIXME probably should be removed after refactoring...
    private void fillChartFromBars(List<TimeSlotBarWithTimeStamp> answer, SymbolChart retval, int indexOffset) throws InvalidArgumentException {
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

    /**
     * for testability only
     * @param facade
     */
    public void setFacade(IDtnIQFeedFacade facade) {
        // FIXME should use a better dependency injection
        this.facade = facade;
    }
    //</editor-fold>

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
                            onTradeQuoteMessage(fields);
                        } else {
                            onNonTradeQuoteMessage(fields); // just in case we reach this my mistake or decide to handle bid/ask updates
                        }
                        break;
                    case 'F':
                        onFundamentalMessage(fields);
                        break;
                    case 'T':
                        onTimeStampMessage(fields);
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

    //<editor-fold desc="level 1 data method (can be implemented by streaming support subclass)">
    protected void onTimeStampMessage(String[] fields) {
        // do nothing
    }

    protected void onNonTradeQuoteMessage(String[] fields) {
        // do nothing
    }

    protected void onTradeQuoteMessage(String[] fields) {
        // do nothing
    }
    //</editor-fold>

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

    // FIXME used only in unit test
    public void setFundamentalsRequested(boolean value) {
        fundamentalsRequested.set(value);
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
            onCustMessage(fields); // verify that the watch list matches the market tree
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


    private void onCustMessage(String[] fields) {
        String delayed = fields[2];
        if (!delayed.equals("real_time")) {
            sendParserAlert("getting delayed data instead of real time. Marking feed as unconnected.", null);
            isConnected.set(false);
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

    public IServicesBundle getHeartBeatServicesBundle() throws InvalidStateException, InvalidArgumentException {
        if (heartBeatServicesBundle == null) {
            heartBeatServicesBundle = config.createServicesBundle();
        }
        return heartBeatServicesBundle;
    }

    public synchronized AsyncFunctionFutureResults<List<TimeSlotBarWithTimeStamp>> getBarsRequestPendingCalls() {
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

    public IDtnIQFeedFacade getFacade() {
        return facade;
    }

    public IDtnIQFeedConfig getConfig() {
        return config;
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



    protected class DayBarCall extends AsyncFunctionCall<List<TimeSlotBarWithTimeStamp>> {
        // FIXME Refactor to use a more pluggable model
        private String symbol;
        private String tag;
        private String from;
        private String to;

        protected DayBarCall(String symbol, String from, String to, String tag) {
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
            facade.sendDayDataRequest(symbol, from, to, tag);
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


    /**
     * The async call for the seconds bar request. the main difference between this and MinutesBarCall is tha the
     * asyncCall here requests ticks from the facade rather than minutes (IQFeed does not do second level granularity)
     */
    protected class SecondsBarCall extends AsyncFunctionCall<List<TimeSlotBarWithTimeStamp>> {
               // FIXME Refactor to use a more pluggable model
        private String symbol;
        private String tag;
        private String from;
        private String to;

        protected SecondsBarCall(String symbol, String from, String to, String tag) {
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

    protected class FundamentalsDataFacadeCall extends AsyncFunctionCall<Fundamentals> {
        private String symbol;

        protected FundamentalsDataFacadeCall(String symbol) {
            this.symbol = symbol;
        }

        /**
         * Performs the asynchronous call (the "body" of the function). If you need argument (as you probably do) then
         * they should be in the constructor.
         *
         * @throws Exception
         */
        @Override
        protected void asyncCall() throws Exception {
            fundamentalsRequested.set(true);
            facade.sendGetFundamentalsRequest(symbol);
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
            return symbol;
        }

        @Override
        public Fundamentals call() throws Exception {
            Fundamentals retval = super.call();
            if (!facade.isWatching(symbol)) {
                // remove the temporary watch
                facade.stopWatchingSymbol(symbol);
            }
            return retval;
        }
    }

    protected class DailyDataFacadeCall extends AsyncFunctionCall<Bar> {
        String symbol;
        String tag;

        protected DailyDataFacadeCall(String symbol, String tag) {
            this.symbol = symbol;
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
            facade.sendGetDailyDataRequest(tag, symbol, 31); // answered in handle31DayBars()
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
