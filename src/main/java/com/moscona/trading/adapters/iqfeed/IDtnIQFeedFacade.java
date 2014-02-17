package com.moscona.trading.adapters.iqfeed;

import com.moscona.exceptions.InvalidArgumentException;
import com.moscona.exceptions.InvalidStateException;
import com.moscona.trading.IConfigInitializable;

/**
 * Created: May 12, 2010 2:40:48 PM
 * By: Arnon Moscona
 * An interface defining the behavior of the direct functionality that drives IQFeed.
 * The main purpose of this interface is to separate between the actual IQFeed API functionality and especially the
 * physical IQConnect process and the logical use of it in our context. It does not fully encapsulate the specification
 * and calls the client back with relatively raw data. In this sens they are both coupled to the IQFeed API at a similar
 * level and this interface does not raise the level of abstraction. It does allow for testing in the absence of and
 * actual IQConnect process and actual sockets.
 */
public interface IDtnIQFeedFacade extends IConfigInitializable<IDtnIQFeedConfig> {

    /**
     * sets the client that will be called back by the facade on various messages
     * @param client
     */
    public void setClient(IDtnIQFeedClient client);

    /**
     * Starts IQConnect and returns when the connection has been established.
     * Starts monitor (daemon) threads for each of the socket connections it manages.
     * Established all socket connections.
     * Runs in active mode, meaning that it insists on owning and monitoring the IQConnect.exe process.
     *
     * @throws InvalidStateException
     */
    public void startIQConnect() throws InvalidStateException;

    /**
     * Starts IQConnect in passive mode and returns when the connection has been established.
     * Starts monitor (daemon) threads for each of the socket connections it manages.
     * Established all socket connections.
     * Does not manage the IQConnect.exe process while in passive mode.
     *
     * @throws InvalidStateException
     */
    public void startIQConnectPassive() throws InvalidStateException;

    /**
     * Starts monitoring any existing owned IQConnect process. If there is none or it is terminated, starts
     * a new one in active mode.
     * @throws InvalidStateException
     */
    public void switchToActiveMode() throws InvalidStateException;

    /**
     * Assuming that the facade is started already - requests a reconnection
     * @throws InvalidStateException
     */
    public void requestConnect() throws InvalidStateException;

    /**
     * Stops IQConnect, closes all sockets connections, and kills the sub-process.
     * Stops all the socket monitor threads.
     * @throws InvalidStateException
     */
    public void stopIQConnect() throws InvalidStateException;

    /**
     * Start watching a named symbol
     * @throws InvalidStateException
     */
    public void startWatchingSymbol(String symbol) throws InvalidStateException;

    /**
     * Stops watching a named symbol
     * @throws com.moscona.exceptions.InvalidStateException
     */
    public void stopWatchingSymbol(String symbol) throws InvalidStateException;

    /**
     * Called by the client when a system message is received and not handled directly by the client
     * @param fields
     * @throws InvalidStateException
     */
    public void onSystemMessage(String[] fields) throws InvalidStateException;

    /**
     * Called by the client when it gets a confirmation of connection established
     */
    public void onConnectionEstablished();

    /**
     * Send a request to get daily data on the symbol. Uses the HDX command as documented in
     * http://www.iqfeed.net/dev/api/docs/HistoricalviaTCPIP.cfm
     * @param responseTag - the sync-over-async response code to attach to this request (and the response)
     * @param symbol - the symbol the query is about
     * @param maxPoints - the maximum data points to retrieve
     * @throws InvalidStateException - if any exception occures
     * @throws InvalidArgumentException - if any of the arguments is bad
     */
    void sendGetDailyDataRequest(String responseTag, String symbol, int maxPoints) throws InvalidStateException, InvalidArgumentException;

    /**
     * Send a request for fundamentals information (over the level1 streaming port)
     * @param symbol
     * @throws InvalidStateException
     */
    void sendGetFundamentalsRequest(String symbol) throws InvalidStateException;

    boolean isWatching(String symbol);

    void doEmergencyRestart() throws InvalidStateException;

    void sendDayDataRequest(String symbol, String from, String to, String requestId) throws Exception;

    void sendMinuteDataRequest(String symbol, String from, String to, String requestId) throws Exception;

    void sendTickDataRequest(String symbol, String from, String to, String requestId) throws Exception;

    void unwatchAll() throws InvalidStateException;
}
