package com.moscona.trading.adapters.iqfeed;

import com.moscona.util.app.lifecycle.events.ApplicationFatalErrorEvent;
import com.moscona.util.app.lifecycle.events.BeforeApplicationStartEvent;
import com.moscona.util.collections.CappedArrayBuffer;
import com.moscona.exceptions.InvalidArgumentException;
import com.moscona.exceptions.InvalidStateException;
import com.moscona.util.TimeHelper;
import com.moscona.util.concurrent.ConfiguredDaemonThread;
import com.moscona.util.windows.TaskList;
import com.moscona.util.IAlertService;
import com.moscona.util.SafeRunnable;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Invoke;
import org.apache.commons.lang3.StringUtils;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created: May 12, 2010 2:42:21 PM
 * By: Arnon Moscona
 * An actual implementation of the facade (the interface is needed for easy creation of mocks)
 */
@SuppressWarnings({"UnusedDeclaration"}) // dependency injection
public class DtnIQFeedFacade implements IDtnIQFeedFacade {
    public static final int ADMIN_PORT = 9300;
    public static final int LEVEL1_PORT = 5009;
    public static final int LEVEL1_LOOKUP_PORT = 9100;
    //public static final int LEVEL1_LOOKUP_PORT = 9100;

    public static final int ADMIN_BUFFER_SIZE = 1024*2;
    public static final int LEVEL1_LOOKUP_BUFFER_SIZE = 1024*2;
    public static final int LEVEL1_BUFFER_SIZE = 128; //1024*1024;  // IT-245 experimenting with small level1 buffers

    /**
     * The app name registered for the IQFeed API
     */
    public static final String APP_NAME = "ARNON_MOSCONA_1407";
    public static final String APP_VERSION = "0.1";

    @SuppressWarnings({"FieldCanBeLocal"})
    private IDtnIQFeedConfig config = null;
    private IDtnIQFeedClient client=null;
    private Process iqConnect=null;
    private String subscriberLogin=null;
    private String subscriberPassword=null;
    private int lookupThreadCount=4; // the number of lookup connections we maintain with IQConnect
    private boolean debugLoggingEnabled=false;

    private LocalSocketConnection admin=null;
    private LocalSocketConnection level1=null;
    private List<LocalSocketConnection> lookupChannelPool = null;
    private int currentLookupChannelNumber = 0;
    private final Object lookupChannelMonitor = new Object(); // used for synchronization only

    //private LocalSocketConnection lookup=null;

    private AtomicBoolean isShutdown;
    private AtomicBoolean isIqConnectDead;
    private AtomicBoolean isIqConnectDestroyed;
    private static final String IQCONNECT_EXE = "iqconnect.exe";

    private final ReentrantLock level1ConnectedLock;
    private final Condition level1ConnectedCondition;
    private AtomicBoolean level1ConnectionProcessStarted;

    private ReentrantLock startStopLock;
    private Set<String> watchList;

    @SuppressWarnings({"UnusedDeclaration"})
    public DtnIQFeedFacade() {
        isShutdown = new AtomicBoolean(false);
        isIqConnectDead = new AtomicBoolean(true);
        isIqConnectDestroyed = new AtomicBoolean(false);
        level1ConnectedLock = new ReentrantLock();
        level1ConnectedCondition = level1ConnectedLock.newCondition();
        level1ConnectionProcessStarted = new AtomicBoolean(false);
        startStopLock = new ReentrantLock();
        watchList = new CopyOnWriteArraySet<String>();
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
    public void init(IDtnIQFeedConfig config) throws InvalidArgumentException, InvalidStateException {  // FIXME need to refactor config and decouple from Intellitrade
        this.config = config;
        try {
            Map myConfig = (Map)config.getComponentConfigFor("DtnIQFeedFacade");
            subscriberLogin = config.simpleComponentConfigEntryWithOverride("DtnIQFeedFacade", "subscriberLogin");
            subscriberPassword = config.simpleComponentConfigEntryWithOverride("DtnIQFeedFacade","subscriberPassword");
            String lookupThreadCountStr = config.simpleComponentConfigEntryWithOverride("DtnIQFeedFacade","lookupThreadCount");
            Object debugModeEnabled = myConfig.get("debugModeEnabled");
            this.debugLoggingEnabled = debugModeEnabled != null && debugModeEnabled.equals("true");

            if (StringUtils.isNotBlank(lookupThreadCountStr) && StringUtils.isNumeric(lookupThreadCountStr)) {
                lookupThreadCount = Integer.parseInt(lookupThreadCountStr);
            }

            config.getServicesBundle().getEventPublisher().subscribe(this); // we're listening for BeforeApplicationStartEvent
//            config.getEventPublisher().onEvent(EventPublisher.Event.SERVER_STARTING, new SafeRunnable(){
//                @Override
//                public void runWithPossibleExceptions() throws Exception {
//                    if (getIqConnectDead()) {
//                        startIQConnect();
//                    }
//                }
//            }.setName("IQFeedFacade start IQConnect"));
        } catch (Exception e) {
            throw new InvalidArgumentException("Unable to initialize from configuration: "+e,e);
        }
    }

    @Handler(delivery = Invoke.Asynchronously)
    public void handleApplicationStartEvent(BeforeApplicationStartEvent event) {
        if (getIqConnectDead()) {
            try {
                startIQConnect();
            }
            catch (InvalidStateException e) {
                ApplicationFatalErrorEvent errorEvent = new ApplicationFatalErrorEvent(e);
                config.getServicesBundle().getEventPublisher().publishAsync(errorEvent);
            }
        }
    }

    /**
     * sets the client that will be called back by the facade on various messages
     *
     * @param client
     */
    @Override
    public void setClient(IDtnIQFeedClient client) {
        this.client = client;
    }

    /**
     * Starts IQConnect and returns when the connection has been established.
     * Starts monitor (daemon) threads for each of the socket connections it manages.
     * Established all socket connections.
     *
     * @throws com.moscona.exceptions.InvalidStateException
     *
     */
    @Override
    public void startIQConnect() throws InvalidStateException {
        startIQConnect(true);
    }

    public void startIQConnect(boolean startDeathWatch) throws InvalidStateException {
        // synchronized with priority over death watch
        startStopLock.lock();
        try {
            try {
                killAllIqConnectSystemProcesses();


                String command = "IQConnect.exe -product " + APP_NAME + " -version " +
                        APP_VERSION +
                        " -login " +
                        subscriberLogin +
                        " -password " +
                        subscriberPassword +
                        " -autoconnect";
                log("Starting IQConnect client:\n"+command);
                iqConnect = Runtime.getRuntime().exec(command);
                log("IQConnect should be running now...");
                isIqConnectDead.set(false);
                isIqConnectDestroyed.set(false);
                if (startDeathWatch) {
                    startDeathWatch();
                }
            } catch (Exception e) {
                String msg = "Exception when trying to start IQConnect: " + e;
                log(msg);
                throw new InvalidStateException(msg,e);
            }
            requestConnect();
        }
        finally {
            startStopLock.unlock();
        }
    }

    /**
     * Starts IQConnect in passive mode and returns when the connection has been established.
     * Starts monitor (daemon) threads for each of the socket connections it manages.
     * Established all socket connections.
     * Does not manage the IQConnect.exe process while in passive mode.
     *
     * @throws com.moscona.exceptions.InvalidStateException
     *
     */
    @Override
    public void startIQConnectPassive() throws InvalidStateException {
        // synchronized with priority over death watch
        startStopLock.lock();
        try {
            if (new TaskList().listTasks(IQCONNECT_EXE).size()>0) {
                isIqConnectDestroyed.set(true); // prevent auto-restart
                isIqConnectDead.set(false); // fake it
                requestConnect();
            }
            else {
                startIQConnect(false);
            }
        } finally {
            startStopLock.unlock();
        }
    }

    /**
     * Starts monitoring any existing owned IQConnect process. If there is none or it is terminated, starts
     * a new one in active mode.
     *
     * @throws com.moscona.exceptions.InvalidStateException
     *
     */
    @Override
    public void switchToActiveMode() throws InvalidStateException {
        //FIXME implement DtnIQFeedFacade.switchToActiveMode
    }

    private synchronized void killAllIqConnectSystemProcesses() throws InvalidStateException, InterruptedException {
        TaskList taskList = new TaskList();
        List<TaskList.TaskInfo> tasks = taskList.listTasks(IQCONNECT_EXE);
        IAlertService alertService = config.getServicesBundle().getAlertService();

        if (tasks.size()>0) {
            isIqConnectDestroyed.set(true); // prevent the watchdog (death watch) from attempting to restart the IQConnect process
            if (alertService != null) {
                alertService.sendAlert("Found existing iqConnect processes. Attempting to kill them...");
            }
            for (TaskList.TaskInfo task: tasks) {
                if (alertService != null) {
                    alertService.sendAlert("  killing PID "+task.getPid());
                }
                taskList.tryToKill(task.getPid());
                Thread.sleep(100);
            }
        }

        // wait for them to be killed
        int startedWaiting = TimeHelper.now();
        while (TimeHelper.now()-startedWaiting<2000 && taskList.listTasks(IQCONNECT_EXE).size()>0) {
            if (alertService!=null)
                alertService.sendAlert("Still waiting for IQConnect processes to die...");
            Thread.sleep(100);
        }
        if (taskList.listTasks(IQCONNECT_EXE).size()>0 && alertService!=null) {
            alertService.sendAlert("Was unable to complete kill! Expect problems...");
        }
    }

    /**
     * Starts a watch thread that waits for IQConnect to die and decide what to do about it
     */
    private void startDeathWatch() {
        new ConfiguredDaemonThread<IDtnIQFeedConfig>(new Runnable() {
            @Override
            public void run() {
                try {
                    iqConnect.waitFor();
                    isIqConnectDead.set(true);
                    onIqConnectDeath();
                } catch(InterruptedException e) {
                    config.getAlertService().sendAlert("Interrupted while waiting for IQConnect to die - killing it");
                    try {
                        stopIQConnect();
                    } catch(InvalidStateException e1) {
                        // do nothing
                    }
                }
            }
        }, "IQConnect death watch daemon", config).start();
    }

    private void onIqConnectDeath() {
        try {
            if(startStopLock.tryLock(0,TimeUnit.SECONDS)) {
                try {
                    if (! isIqConnectDestroyed.get()) {
                        // we did not kill the guys. We need to restart it and notify everybody that we had this problem
                        String message = "IQConnect seems to have died unexpectedly! Attempting a restart";
                        //noinspection ThrowableInstanceNeverThrown
                        config.getAlertService().sendAlert(message, new InvalidStateException(message));
                        try {
                            startIQConnect();
                        } catch (InvalidStateException e) {
                            config.getAlertService().sendAlert("Exception while trying to restart IQConnect",e);
                        }
                    }
                } finally {
                    startStopLock.unlock();
                }
            }

        } catch (InterruptedException e) {
            // ignore
        }
    }

    private void setUpAdminConnection() throws InvalidStateException {
        try {
            boolean success = false;
            long retrySleepTime = 1000;
            int retryCount = 10;
            while(!success && retryCount>0) {
                retryCount--;
                try {
                    admin = new LocalSocketConnection("IQConnect admin", ADMIN_PORT, ADMIN_BUFFER_SIZE,config, debugLoggingEnabled);
                }
                catch (Exception e) {
                    if (retryCount<=0) {
                        throw e;
                    }
                    config.getAlertService().sendAlert("Failed to create local connection to IQConnect ("+retryCount+" retries left) waiting "+retrySleepTime+"msec before retry");
                    Thread.sleep(retrySleepTime);
                }
            }

            log("Starting IQConnect admin reader");
            admin.startReaderDaemon(new SafeRunnable() {
                @Override
                public void runWithPossibleExceptions() throws Exception {
//                    debug("Starting daemon", "admin reader");
                    while (!isShutdown.get() && ! Thread.currentThread().isInterrupted()) {
                        try {
                            String rawMessages = admin.read();
//                            debug("Sending data to client",rawMessages);
                            if (rawMessages !=null) {
                                client.onAdminData(rawMessages);
                            }
                        }
                        catch (InterruptedException e) {
                            shutdown();
                            client.onFatalException(e);
                        }
                        catch (Exception e) {
                            client.onException(e, false);
                        }
                    }
                }
            });
        }catch (Exception e) {
            String msg = "Error while establishing the admin connection to IQConnect: " + e;
            log(msg);
            throw new InvalidStateException(msg,e);
        }
    }

    private void shutdown() {
        log("Shutting down IQFeed facade");
        isShutdown.set(true);
        try {
            stopIQConnect();
        } catch (InvalidStateException e) {
            client.onException(e,true);
        }
    }

    private void register() throws InvalidStateException {
        try {
            log("Starting registration with IQConnect...");
            String registrationString = "S,REGISTER CLIENT APP,"+APP_NAME+",0.1";
            admin.write(registrationString);
            String contents = admin.read();

            log("done registering, waiting for confirmation from IQConnect");
        }
        catch (Exception e) {
            client.onFatalException(e);
        }
    }

    /**
     * for debugging
     * @param message
     */
    private void log(String message) {
        // todo logging should be delegated to the client (and probably routed to the GUI somehow) - or maybe publish an event via the event publisher
        System.out.println(message); // FIXME DELETE THIS LINE!!!
        if (config!= null) {
            IAlertService alerts = config.getAlertService();
            if (alerts != null) {
                alerts.sendAlert(message);
            }
        }
    }

    /**
     * Assuming that the facade is started already - requests a reconnection
     *
     * @throws com.moscona.exceptions.InvalidStateException
     *
     */
    @Override
    public void requestConnect() throws InvalidStateException {
        // todo verify status of IQconnect IT-130
        setUpAdminConnection();
        register();
    }

    /**
     * Stops IQConnect, closes all sockets connections, and kills the sub-process.
     * Stops all the socket monitor threads.
     *
     * @throws com.moscona.exceptions.InvalidStateException
     *
     */
    @Override
    public void stopIQConnect() throws InvalidStateException {
        // synchronized with priority over death watch
        startStopLock.lock();
        try {
            InvalidStateException ex = null;

            // don't bail out on any of the exceptions
            try {
                admin.close();
            } catch (InvalidStateException e) {
                ex = e;
                log("Exception while closing: "+e);
                client.onException(e,false);
            }

            try {
                level1.close();
            } catch (InvalidStateException e) {
                ex = e;
                log("Exception while closing: "+e);
                client.onException(e,false);
            }

            try {
                isIqConnectDestroyed.set(true);
                iqConnect.destroy();
                // make double sure
                killAllIqConnectSystemProcesses();
            } catch (Exception e) {
                log("Exception while closing: "+e);
                client.onException(e,false);
            }

            if (ex != null) {
                throw(ex);
            }
        } finally {
            startStopLock.unlock();
        }
    }

    /**
     * Start watching a named symbol
     *
     * @throws com.moscona.exceptions.InvalidStateException
     *
     */
    @Override
    public void startWatchingSymbol(String symbol) throws InvalidStateException {
        try {
            if (level1==null) {
                awaitLevel1ConnectionSignal();
            }
            level1.write("w"+symbol);
            watchList.add(symbol);
        } catch (Exception e) {
            String msg = "Error while adding watch on '" + symbol + "' :" + e;
            log(msg);
            throw new InvalidStateException(msg,e);
        }
    }

    @Override
    public void unwatchAll() throws InvalidStateException {
        try {
            if (level1==null) {
                awaitLevel1ConnectionSignal();
            }
            level1.write("S,UNWATCH ALL");
            watchList.clear();
        } catch (Exception e) {
            String msg = "Error while removing all watches using 'S,UNWATCH ALL' :" + e;
            log(msg);
            throw new InvalidStateException(msg,e);
        }
    }

    /**
     * Stops watching a named symbol
     *
     * @throws com.moscona.exceptions.InvalidStateException
     *
     */
    @Override
    public void stopWatchingSymbol(String symbol) throws InvalidStateException {
        try {
            level1.write("r"+symbol);
            watchList.remove(symbol);
        } catch (Exception e) {
            String msg = "Error while removing watch on '" + symbol + "' :" + e;
            log(msg);
            throw new InvalidStateException(msg,e);
        }
    }

    // FIXME implement a method for sending a fundamentals request (on streaming connection)
    // FIXME implement a method for sending a daily data request (HDX on lookup) request

    /**
     * Called by the client when a system message is received and not handled directly by the client
     * @param fields
     * @throws InvalidStateException
     */
    @Override
    public void onSystemMessage(String[] fields) throws InvalidStateException {
        if (fields[1].equals("REGISTER CLIENT APP COMPLETED")) {
            onRegistrationComplete();
        }
    }

    private void onRegistrationComplete() {
        try {
            level1Connect();
        } catch (InvalidStateException e) {
            String msg = "Fatal error after registration complete: " + e;
            log(msg);
            client.onFatalException(e);
        }
    }

    @Override
    public void onConnectionEstablished() {
        try {
            if (level1==null) {
                if (level1ConnectionProcessStarted.get()) {
                    config.getAlertService().sendAlert("Level1 not yet connected. Waiting...");
                    awaitLevel1ConnectionSignal();
                }
                else {
                    level1Connect();
                    if (level1==null)
                        awaitLevel1ConnectionSignal();
                }
            }
            log("IQConnect level1 connection established...");
            level1RequestFundamentalFieldNames();
            level1SelectFields();
        }
        catch (Exception e) {
            log("Fatal error while completing post-connection activity: "+e);
            client.onFatalException(e);
        }
    }

    private void awaitLevel1ConnectionSignal() throws InterruptedException, InvalidStateException {
        level1ConnectedLock.lock();
        try {
            if (! level1ConnectedCondition.await(5, TimeUnit.SECONDS)) {
                throw new InvalidStateException("Never got a level1 connection within timeout. This connection is not usable...");
            }
        }
        finally {
            level1ConnectedLock.unlock();
        }
    }

    private void level1RequestFundamentalFieldNames() {
        log("Requesting fundamental field list names");
        try {
            level1.write("S,REQUEST FUNDAMENTAL FIELDNAMES");
        } catch (Exception e) {
            log("Exception while requesting fundamental field names: "+e);
            client.onException(e,true);
        }
    }

    /**
     * Selects which fields are used for ticks
     */
    private void level1SelectFields() {
        try {
            log("Changing field list");
            String[] fields = {
                    "Last Trade Time",
                    "Delay",
                    "Symbol",
                    "Last",
                    "Incremental Volume",
                    "Total Volume",
                    //                "Ask",
                    //                "Bid",
                    //                "Ask Size",
                    //                "Bid Size"
            };

            String command = "S,SELECT UPDATE FIELDS";
            for (String field: fields) {
                command += ","+field;
            }
            level1.write(command);

            log("Requesting field order");
            // the following will result in something like:
            // S,CURRENT UPDATE FIELDNAMES,Symbol,Last,Total Volume,Incremental Volume,Bid,Ask,Bid Size,Ask Size,Last Trade Time,Delay
            level1.write("S,REQUEST ALL UPDATE FIELDNAMES"); // ask to get the field order
        } catch (Exception e) {
            // This is not a critical error but we should take notice of it
            String msg = "Error while sending commands to level1 connection: "+e;
            log(msg);
            client.onException(e,true); // saying it's handled because there's nothing more to do here
        }
    }

    private void level1Connect() throws InvalidStateException {
        // FIXME need to connect both to streaming and lookup connections
        level1Connect(3); // retry up to 3 times
        if (level1!= null) {
            signalLevel1Connected();
        }
    }

    private void signalLevel1Connected() {
        level1ConnectedLock.lock();
        try {
            level1ConnectedCondition.signalAll();
        }
        finally {
            level1ConnectedLock.unlock();
        }
    }

    private void level1Connect(int depthCharge) throws InvalidStateException {
        boolean retry = (depthCharge>0);
        boolean hadException = false;

        try {
            level1ConnectionProcessStarted.set(true);
            level1 = new LocalSocketConnection("IQConnect level1", LEVEL1_PORT, LEVEL1_BUFFER_SIZE, config, debugLoggingEnabled);
            log("Starting IQConnect level1 reader");
            level1.startReaderDaemon(new SafeRunnable() {
                @Override
                public void runWithPossibleExceptions() throws Exception {
//                    debug("Starting daemon","level1 reader");
                    while (!isShutdown.get() && ! Thread.currentThread().isInterrupted()) {
                        try {
                            String rawMessages = level1.read(true);
                            //todo add read timing tracking here
//                            debug("Sending data to client",rawMessages);
                            if (rawMessages!=null) {
                                client.onLevelOneData(rawMessages);
                                //todo add queue add timing tracking here
                            }
                        }
                        catch (InterruptedException e) {
                            shutdown();
                            client.onFatalException(e);
                        }
                        catch (Exception e) {
                            client.onException(e, false);
                        }
                    }
                }
            });
        }
        catch (Exception e) {
            String msg = "Error while establishing the level1 connection to IQConnect: " + e;
            log(msg);
            hadException = true;
            if (!retry) {
                throw new InvalidStateException(msg,e);
            }
        }

        if (hadException && retry) {
//            debug("level1Connect","retry after .5 sec pause");
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new InvalidStateException("Interrupted while retrying connection to level1 port");
            }
            level1Connect(depthCharge-1);
        }
    }

    public boolean getIqConnectDead() {
        return isIqConnectDead.get();
    }

    /**
     * Ensure that we're connected and then sends requested command
     * @param command
     * @throws Exception
     */
    private void sendLookupCommand(String command) throws Exception {
        ensureLookupIsConnected();
        LocalSocketConnection lookup = rotateLookupChannel();
        lookup.write(command+LocalSocketConnection.CRLF);
    }

    private void ensureLookupIsConnected() throws Exception {
        if (lookupChannelPool == null) {
            synchronized (lookupChannelMonitor) {
                if (lookupChannelPool != null) {
                    //log("pool already created via another thread...");
                    return;
                }

                try {
                    ArrayList<LocalSocketConnection> lookupChannels = new ArrayList<LocalSocketConnection>();
                    for (int i=0; i<lookupThreadCount; i++) {
                        LocalSocketConnection channel = new LocalSocketConnection("IQConnect level1 lookup channel "+i, LEVEL1_LOOKUP_PORT, LEVEL1_LOOKUP_BUFFER_SIZE, config, debugLoggingEnabled);
                        //log("starting IQConnect lookup socket reader "+i);
                        channel.startReaderDaemon(new LookupReader(channel));
                        lookupChannels.add(channel);
                    }
                    lookupChannelPool = new CopyOnWriteArrayList<LocalSocketConnection>(lookupChannels);
                    currentLookupChannelNumber = 0;
                }
                catch (Exception e) {
                    String msg = "Error while establishing the lookup connection to IQConnect: " + e;
                    log(msg);
                    throw new InvalidStateException(msg,e);
                }
            }
        }
    }

    private LocalSocketConnection rotateLookupChannel() throws Exception {
        LocalSocketConnection retval = null;
        synchronized (lookupChannelMonitor) {
            ensureLookupIsConnected();
            currentLookupChannelNumber++;
            if (currentLookupChannelNumber >= lookupChannelPool.size()) {
                currentLookupChannelNumber = 0;
            }
            retval = lookupChannelPool.get(currentLookupChannelNumber);
        }
        return retval;
    }

    /**
     * Daily close data - used for market tree updates.
     * @param responseTag
     * @param symbol
     * @param maxPoints
     * @throws com.moscona.exceptions.InvalidArgumentException
     * @throws com.moscona.exceptions.InvalidStateException
     */
    @Override
    public void sendGetDailyDataRequest(String responseTag, String symbol, int maxPoints) throws InvalidStateException, InvalidArgumentException {
        if (responseTag.contains(",")) {
            throw new InvalidArgumentException("response tag may not include commas");
        }
        if (maxPoints < 1) {
            throw new InvalidArgumentException("maxPoints must be at least 1");
        }
        String command = "HDX," + symbol + "," + maxPoints + ",0," + responseTag + ",100";
        try {
            sendLookupCommand(command);
        } catch (Exception e) {
            throw new InvalidStateException("Exception sending lookup command: "+command, e);
        }
    }

    /**
     * Explicit fundamentals request. Used for market tree updates.
     * @param symbol
     * @throws com.moscona.exceptions.InvalidStateException
     */
    @Override
    public void sendGetFundamentalsRequest(String symbol) throws InvalidStateException {
        try {
            if (! isWatching(symbol)) {
                // watch temporarily and don't add to watch list
                level1.write("w" + symbol + level1.CRLF);
            }
            String command = "f" + symbol + level1.CRLF;
            level1.write(command);
        } catch (Exception e) {
            throw new InvalidStateException("Exception while trying to send a fundamentals request for "+symbol+" :"+e,e);
        }
    }

    @Override
    public boolean isWatching(String symbol) {
        return watchList.contains(symbol);
    }

    @Override
    public void doEmergencyRestart() throws InvalidStateException {
        stopIQConnect();
        startIQConnect();
    }

    /**
     * Requests lookup data for minute data. Generally translates into a HIT request.
     * This is here mostly to help testability a bit.
     * @param symbol the symbol
     * @param from an IQFeed timestamp e.g. "20100708 093000"
     * @param to an IQFeed timestamp e.g. "20100708 093000"
     * @param requestId the string to identify the response by
     */
    @Override
    public void sendDayDataRequest(String symbol, String from, String to, String requestId) throws Exception {
        // HDT,CLB,20090731,20100731,265,1,oneYear CLB,265
        StringBuilder request = new StringBuilder();
        request.append("HDT,").append(symbol).append(",");
        request.append(from).append(",").append(to).append(",,1,").append(requestId);
        sendLookupCommand(request.toString());
    }

    /**
     * Requests lookup data for minute data. Generally translates into a HIT request.
     * This is here mostly to help testability a bit.
     * @param symbol the symbol
     * @param from an IQFeed timestamp e.g. "20100708 093000"
     * @param to an IQFeed timestamp e.g. "20100708 093000"
     * @param requestId the string to identify the response by
     */
    @Override
    public void sendMinuteDataRequest(String symbol, String from, String to, String requestId) throws Exception {
        // "HIT,"+symbol+",60," + beginTs+"," + endTs + ",,,,1,Partial day minutes: "+symbol
        StringBuilder request = new StringBuilder();
        request.append("HIT,").append(symbol).append(",60,");
        request.append(from).append(",").append(to).append(",,,,1,").append(requestId);
        sendLookupCommand(request.toString());
    }

    /**
     * Requests lookup data for tick data. Generally translates into a HTT request (ticks filtered to trading day only).
     * This is here mostly to help testability a bit.
     * @param symbol the symbol
     * @param from an IQFeed timestamp e.g. "20100708 093000"
     * @param to an IQFeed timestamp e.g. "20100708 093000"
     * @param requestId the string to identify the response by
     */
    @Override
    public void sendTickDataRequest(String symbol, String from, String to, String requestId) throws Exception {
        // "HTT,"+symbol+","+beginTs+","+endTs+",,093000,160000,1,"+description
        StringBuilder request = new StringBuilder();
        request.append("HTT,").append(symbol).append(",");
        request.append(from).append(",").append(to).append(",,093000,160000,1,").append(requestId);
        sendLookupCommand(request.toString());
    }

    protected static class DebugLog {
        private CappedArrayBuffer<DebugLogItem> log;
        private String filename;

        protected DebugLog(String filename) {
            this.filename = filename;
            log = new CappedArrayBuffer<DebugLogItem>(100);
        }

        public void log(String info) {
            log.add(new DebugLogItem(info));
        }

        public void dumpToFile() {
            try {
                PrintStream out = new PrintStream(new FileOutputStream(filename));
                try {
                    for (DebugLogItem item: log) {
                        out.println(Long.toString(item.getTimestamp())+","+item.getInfo());
                    }
                }
                catch(Throwable t) {
                    // ignore
                }
                finally {
                    out.close();
                }
            }
            catch (FileNotFoundException e) {
                System.err.println("Exception while dumping log "+e.getClass().getName()+": "+e);
            }
        }
    }

    protected static class DebugLogItem {
        private long timestamp;
        private String info;

        protected DebugLogItem(String info) {
            timestamp = System.currentTimeMillis();
            this.info = info;
        }

        public String getInfo() {
            return info;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    /**
     * A class that bundles the resources for the socket connections used by the facade
     */
    protected static class LocalSocketConnection {
        @SuppressWarnings({"ConstantNamingConvention"})
        protected static final String CRLF = "\r\n";
        @SuppressWarnings({"MagicNumber"})
        protected static final byte[] LOOPBACK = {127,0,0,1}; //"127.0.0.1";

        protected InetSocketAddress address;
        protected SocketChannel channel;
        protected ByteBuffer buffer;

        private final CharsetEncoder encoder;
        private final CharsetDecoder decoder;

        private Thread readerThread = null;
        private String name=null;
        private String remainder=""; // carry-over from one packet to the next (any tail with no linefeed)
        private final ReentrantLock readLock;
        private final ReentrantLock writeLock;
        private IDtnIQFeedConfig config;
        private DebugLog debugLog; // created due to IT-383
        private boolean debugLoggingEnabled=false; // created due to IT-383

        protected LocalSocketConnection(String name, int port, int bufferSize, IDtnIQFeedConfig config, boolean debugLoggingEnabled) throws Exception {
            this.name = name;
            this.config = config;

            address = new InetSocketAddress(InetAddress.getByAddress(LOOPBACK), port);
            channel = SocketChannel.open();
            channel.connect(address);
            channel.configureBlocking(true);
            buffer = ByteBuffer.allocateDirect(bufferSize);

            Charset charset = Charset.forName("US-ASCII");
            decoder = charset.newDecoder();
            encoder = charset.newEncoder();
            readLock = new ReentrantLock();
            writeLock = new ReentrantLock();

            debugLog = new DebugLog(name+".debug.log");
            this.debugLoggingEnabled = debugLoggingEnabled;
        }

        private void debugLog(String info) {
            if (debugLoggingEnabled) {
                debugLog.log(info);
            }
        }

        private void dumpLogToFile() {
            if (debugLoggingEnabled) {
                debugLog.dumpToFile();
            }
        }

        protected void write(String command) throws Exception {
            debugLog.log("write(\""+command+"\")");

            writeLock.lock();
            try {
                synchronized (encoder) {
                    channel.write(encoder.encode(CharBuffer.wrap(command + CRLF)));
                }
            }
            catch (Exception t) {
                debugLog("Exception in write(): "+t);
                dumpLogToFile();
                throw t;
            }
            finally {
                writeLock.unlock();
            }
        }

        protected String read() throws Exception {
            return read(false);
        }

        protected String read(boolean debug) throws Exception {
            readLock.lock();
            debugLog("read()");

            try {
                String result = remainder;

                synchronized (buffer) {
                    buffer.clear();
                    channel.read(buffer);
                    //todo add read timing tacking here
                    buffer.flip();

                    synchronized (decoder) {
                        result += decoder.decode(buffer).toString();
                    }
                }
                //            debug(name+" received", result);

                // make sure that the results are made up from whole lines, leaving any left-over to stick on the next
                // packet
                remainder = "";
                int lastLineFeedPosition = result.lastIndexOf('\n');
                if (lastLineFeedPosition == -1) {
                    // no linefeed in the string
                    remainder = result;
                    result = null; //todo make sure that the readers know how to handle nulls
                }
                else if (lastLineFeedPosition<result.length()-1) {
                    // there is a linefeed, and it is not the last character
                    remainder = result.substring(lastLineFeedPosition+1);
                    result = result.substring(0,lastLineFeedPosition+1);
                }
                return result;
            }
            catch (Exception t) {
                debugLog("Exception in read(): "+t);
                dumpLogToFile();
                throw t;
            }
            finally {
                readLock.unlock();
            }

        }

//        private void debug(String heading, String message) {
////            DtnIQFeedFacade.debug(heading,message);
//        }

        public void startReaderDaemon(final SafeRunnable safeRunnable) {
            debugLog("startReaderDaemon()");
            readerThread = new ConfiguredDaemonThread<IDtnIQFeedConfig>(safeRunnable, name+" reader daemon",config);
            readerThread.start();
        }

        public void close() throws InvalidStateException {
            try {
                debugLog("close()");
                channel.close();
            } catch (IOException e) {
                throw new InvalidStateException("Error while closing channel in "+name+": "+e,e);
            }
        }
    }

    /**
     * A runnable used to construct multiple lookup channel readers
     */
    private class LookupReader extends SafeRunnable {
        private LocalSocketConnection channel;

        protected LookupReader(LocalSocketConnection channel) {
            this.channel = channel;
        }

        @Override
        public void runWithPossibleExceptions() throws Exception {
            //                    debug("Starting daemon","level1 reader");
            while (!isShutdown.get() && ! Thread.currentThread().isInterrupted()) {
                try {
                    String rawMessages = channel.read();
                    //                            debug("Sending data to client",rawMessages);
                    if (rawMessages != null) {
                        client.onLookupData(rawMessages);
                    }
                }
                catch (InterruptedException e) {
                    shutdown();
                    client.onFatalException(e);
                }
                catch (Exception e) {
                    client.onException(e, false);
                }
            }
        }
    }

//    private static void debug(String heading, String message) {
//        System.out.println(heading+"\n"+message+"\n\n"); // FIXME DELETE THIS LINE!!!
//    }
}
