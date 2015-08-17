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


import com.moscona.exceptions.InvalidArgumentException;
import com.moscona.exceptions.InvalidStateException;
import com.moscona.trading.IConfigInitializable;
import com.moscona.trading.adapters.IHistoricalDataSource;

/**
 * Created: May 12, 2010 2:43:32 PM
 * By: Arnon Moscona
 * An interface that defines the behavior of the client that the facade interface depends on (event callbacks for the most part).
 */
public interface IDtnIQFeedHistoricalClient extends IConfigInitializable<IDtnIQFeedConfig>, IHistoricalDataSource {
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
    void onLookupData(String rawMessages) throws InvalidStateException, InvalidArgumentException;
}
