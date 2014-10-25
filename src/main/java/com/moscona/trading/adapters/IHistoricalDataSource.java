package com.moscona.trading.adapters;

import com.moscona.exceptions.InvalidArgumentException;
import com.moscona.exceptions.InvalidStateException;
import com.moscona.trading.elements.SymbolChart;
import com.moscona.trading.excptions.MissingSymbolException;
import com.moscona.trading.persistence.SplitsDb;

import java.util.Calendar;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by Arnon on 5/12/2014. A market data source for historical data. Derived from the original IQFeed
 * implementation in another project
 */
public interface IHistoricalDataSource {
    String getName() throws InvalidStateException;

    /**
     * Given a symbol and a splits DB - queries the data source and updates the splits DB with the latest available
     * information about splits for this symbol
     *
     * @param symbol
     * @param splits
     * @param timeout
     * @param timeoutUnit
     * @throws com.moscona.exceptions.InvalidArgumentException
     * @throws com.moscona.exceptions.InvalidStateException
     * @throws com.moscona.trading.excptions.MissingSymbolException
     * @throws java.util.concurrent.TimeoutException
     */
    void updateSplitsFor(String symbol, SplitsDb splits, int timeout, TimeUnit timeoutUnit) throws InvalidArgumentException, InvalidStateException, TimeoutException, MissingSymbolException;  // FIXME Should depend on an interface for splitdb IQ-12

    // FIXME missing hourly bars IQ-12
    // FIXME missing 5 min bars IQ-12

    /**
     * Gets a minute chart for the symbol, possibly with some values missing. Note that the from and to times must both
     * be from the same day, or an exception might be thrown.
     *
     * @param symbol     the symbol for which historic data is requested
     * @param from       the starting time (the beginning of this minute is the beginning of the first minute to be
     *                   retrieved)
     * @param to         the ending time (the end of this minute is the end of the last minute to be retrieved)
     * @param timeout    the maximum time allowed to spend on this operation
     * @param unit       the units for the timeout
     * @param retryLimit the maximum number of allowed retry attempt on errors that justify a retry
     * @return a SymbolChart with the historic data in the time period. Some slots may be null
     * @throws com.moscona.exceptions.InvalidArgumentException
     * @throws com.moscona.exceptions.InvalidStateException
     * @throws java.util.concurrent.TimeoutException
     * @throws MissingSymbolException
     */
    public SymbolChart getDayBars(String symbol, Calendar from, Calendar to, int timeout, TimeUnit unit, int retryLimit) throws InvalidArgumentException, InvalidStateException, TimeoutException, MissingSymbolException;

    /**
     * Gets a minute chart for the symbol, possibly with some values missing. Note that the from and to times must both
     * be from the same day, or an exception might be thrown.
     *
     * @param symbol     the symbol for which historic data is requested
     * @param from       the starting time (the beginning of this minute is the beginning of the first minute to be
     *                   retrieved)
     * @param to         the ending time (the end of this minute is the end of the last minute to be retrieved)
     * @param timeout    the maximum time allowed to spend on this operation
     * @param unit       the units for the timeout
     * @param retryLimit the maximum number of allowed retry attempt on errors that justify a retry
     * @return a SymbolChart with the historic data in the time period. Some slots may be null
     * @throws com.moscona.exceptions.InvalidArgumentException
     * @throws com.moscona.exceptions.InvalidStateException
     * @throws java.util.concurrent.TimeoutException
     * @throws MissingSymbolException
     */
    public SymbolChart getMinuteBars(String symbol, Calendar from, Calendar to, int timeout, TimeUnit unit, int retryLimit) throws InvalidArgumentException, InvalidStateException, TimeoutException, MissingSymbolException;

    /**
     * Gets a second chart for the symbol, possibly with some values missing. Note that the from and to times must both
     * be from the same day, or an exception might be thrown.
     *
     * @param symbol     the symbol for which historic data is requested
     * @param from       the starting time (the beginning of this second is the beginning of the first second to be
     *                   retrieved)
     * @param to         the ending time (the end of this second is the end of the last second to be retrieved)
     * @param timeout    the maximum time allowed to spend on this operation
     * @param unit       the units for the timeout
     * @param retryLimit the maximum number of allowed retry attempt on errors that justify a retry
     * @return a SymbolChart with the historic data in the time period. Some slots may be null
     * @throws InvalidArgumentException
     * @throws InvalidStateException
     * @throws TimeoutException
     * @throws MissingSymbolException
     */
    public SymbolChart getSecondBars(String symbol, Calendar from, Calendar to, int timeout, TimeUnit unit, int retryLimit) throws InvalidArgumentException, InvalidStateException, TimeoutException, MissingSymbolException;
}
