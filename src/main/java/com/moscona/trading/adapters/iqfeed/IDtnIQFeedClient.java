package com.moscona.trading.adapters.iqfeed;


import com.moscona.trading.IConfigInitializable;

/**
 * Created: May 12, 2010 2:43:32 PM
 * By: Arnon Moscona
 * An interface that defines the behavior of the client that the facade interface depends on (event callbacks for the most part).
 */
public interface IDtnIQFeedClient extends IConfigInitializable<IDtnIQFeedConfig> {
    /**
     * Called when the facade completed the connection setup and is ready to receive commands
     */
    public void onConnectionEstablished();

    /**
     * Called when a connection that is supposed to be up is lost
     */
    public void onConnectionLost();

    /**
     * Called by the facade after the tear down process completes
     */
    public void onConnectionTerminated();

    /**
     * Called whenever the facade encounters an exception.
     * @param ex the exception
     * @param wasHandled  whether or not it was handled already
     */
    public void onException(Throwable ex, boolean wasHandled);

    /**
     * Called when the facade receives new data from the admin connection. This is typically more than one message.
     * @param data the data as a String
     */
    public void onAdminData(String data);

    /**
     * Called when the facade receives new data from the level1 connection. This is typically more than one message.
     * @param data the data as a String
     */
    public void onLevelOneData(String data);

    /**
     * A callback for the facade to notify the client that something terrible happened and everything needs to be shut down.
     * @param e
     */
    void onFatalException(Throwable e);


    /**
     * A callback for the facade to notify the client about new data received on the lookup port
     * @param rawMessages
     */
    void onLookupData(String rawMessages);
}
