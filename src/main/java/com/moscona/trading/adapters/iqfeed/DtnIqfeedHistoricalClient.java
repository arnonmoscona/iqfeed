package com.moscona.trading.adapters.iqfeed;

import com.moscona.events.EventPublisher;
import com.moscona.threads.RealTimeProviderConnectionThread;
import com.moscona.trading.IServicesBundle;
import com.moscona.util.SafeRunnable;
import com.moscona.util.async.AsyncFunctionFutureResults;
import com.moscona.exceptions.InvalidArgumentException;
import com.moscona.exceptions.InvalidStateException;
import com.moscona.trading.elements.Bar;
import com.moscona.trading.streaming.TimeSlotBarWithTimeStamp;
import com.moscona.util.TimeHelper;
import com.moscona.util.collections.Pair;
import com.moscona.util.monitoring.MemoryStateHistory;
import org.apache.commons.lang3.StringUtils;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
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


    public static final String TAG_PREFIX_31_DAY = "31 day ";
    public static final int HANDLER_31_DAY_BARS = 0; // handler ID for the data handler for 31 day bars request
    public static final int HANDLER_MINUTE_BARS = 1; // handler ID for the data handler for minutes bars request
    public static final int HANDLER_TICKS_TO_SECOND_BARS = 2; // handler ID for the data handler for second bars request
    public static final int HANDLER_DAY_BARS = 3; // handler ID for the data handler for day bars request

    public static final String NAME = "IQFeed";

    private IDtnIQFeedFacade facade = null;

    private AtomicBoolean daemonsShouldBeRunning;
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

            config.getEventPublisher().onEvent(EventPublisher.Event.TRADING_CLOSED, new SafeRunnable() {
                @Override
                public void runWithPossibleExceptions() throws Exception {
                    stopWatchingSymbols();
                }
            }.setName("IQFeed stop watching symbols"));

            heartBeatServicesBundle = config.createServicesBundle();
            startHeartBeat();
            listenForStatsUpdateRequests();

            staleTickLogger = new StaleTickLogger(config);

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

    // ==============================================================================================================
    // Private methods
    // ==============================================================================================================

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
}
