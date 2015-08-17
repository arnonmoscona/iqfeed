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

package com.moscona.trading.adapters.iqfeed.lookup;

import com.moscona.trading.adapters.iqfeed.IDtnIQFeedHistoricalClient;

/**
 * Created by Arnon on 5/17/2014.
 * An aggregator of raw IQFeed line responses to be used as an interface between async lookup functions
 * and the IQFeed client
 */
public interface IQFeedResponseLineAggregator {
    /**
     * The core aggregation function. The aggregator is assumed to be stateful and it gets individual IQFeed responses
     * to the original request as lines broken up into fields. It may interpret the fields individually, or may use
     * IQFeed client services to parse the response further. This interface provides the simplest and most flexible way
     * to get the data at the price of having to do further actions for parsing.
     * @param caller the calling client. Can be used for calls to parsing methods or other needs should the aggregator not know which client it is associated with.
     * @param line
     */
    public void add(IDtnIQFeedHistoricalClient caller, String[] line);

    /**
     * This method is called when IQFeed send the last "marker" line signalling the end of the data stream. The marker
     * line itself is not sent to the aggregator using add()
     */
    public void completeAggregation();

    /**
     * Sent to the aggregator on error for a possibly abnormal completion. This could be either an error message from
     * the remote IQFeed or some other error condition (disconnect, shutdown, or other problems)
     * @param literalError nullable. Usually an error from the remote IQFeed as it appeared in the response
     * @param isRemoteError true if the error is a detected error on the IQFeed response stream
     * @param isFatal if true then the aggregator is advised to immediately complete exceptionally. Should not be ignored.
     * @param exception nullable. If present, this is the exception that was thrown in association with this error
     */
    public void addError(String literalError, boolean isRemoteError, boolean isFatal, Throwable exception);

    /**
     * A query to the aggregator to ask whether it is done with the job. An aggregator may declare that it's done
     * before seeing the last response in the stream (before the call to completeAggregation()). It may also decide
     * that it's done after an error (although it is not assumed to always do so). The client will presumably check this
     * periodically, for instance after add() or addError(), but is not required to do so. The client is not required
     * to stop sending data after the aggregator is complete, but it is likely to do so.
     * @return true if completed
     */
    public boolean isCompleted();

    /**
     * A more functional way to signal to the IQFeed client that the aggregation is done for any reason
     * (normal completion, exceptional completion, or whatever trigger). This way the client does not need to poll,
     * but would naturally clean up immediately after completion.
     * @param action an action to perform after completion
     */
    public void whenDone(Runnable action);

    /**
     * A command to the aggregator to reset itself to the initial state. When calls implement a retry policy, then
     * the same aggregator may be reused for multiple calls. While implementations may choose to do otherwise, depending
     * on the particulars of the retry behavior, the aggregator should be coded defensively and allow this reuse
     * behavior.
     *
     * This method may or may not be called before the first time the aggregator is used. Either is legal.
     */
    public void reset();
}
