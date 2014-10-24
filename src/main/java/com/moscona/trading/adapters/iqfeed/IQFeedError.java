package com.moscona.trading.adapters.iqfeed;

/**
 * Created by arnon on 10/2/2014.
 * Refactored out of DTNIqfeedHistoricalClient
 */
public class IQFeedError extends Exception {
    private String literalError;
    private boolean isRetriable = true;

    public IQFeedError(String literalError, String message, boolean isRetriable) {
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