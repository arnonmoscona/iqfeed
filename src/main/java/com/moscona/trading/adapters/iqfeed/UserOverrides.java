package com.moscona.trading.adapters.iqfeed;

/**
 * Created: May 4, 2010 8:55:02 AM
 * By: Arnon Moscona
 * This class represents the configuration options that can be controlled by the user and generally are
 * contributed via user preferences and the configuration UI. It can be combined with the YAML based configuration
 * to produce a working configuration.
 * This is simple bean. It is combined with the server config by the server config itself.
 * FIXME remove unused stuff
 */
public class UserOverrides {
    private String marketTreePath;
    private String tickStreamSimulatorDataPath;
    private String loggingAlertServiceLogFile;
    private String streamDataStoreRoot;
    private String subscriberLogin;
    private String subscriberPassword;
    private String dtnIQFeedClientMode;
    private Integer lookupThreadCount;

    // IDB overrides
    private int idbPerSymbolRetryLimit=-1;
    private int idbRetryLimit=-1;
    private int idbAllowedFailedSymbolsPercent=-1;
    private long idbOverallTimeoutSeconds=-1;

    // SGF overrides
    private int sgfPerSymbolRetryLimit=-1;
    private int sgfRetryLimit=-1;
    private int sgfAllowedFailedSymbolsPercent=-1;
    private long sgfOverallTimeoutSeconds=-1;

    public String getMarketTreePath() {
        return marketTreePath;
    }

    public void setMarketTreePath(String marketTreePath) {
        this.marketTreePath = marketTreePath;
    }

    public String getTickStreamSimulatorDataPath() {
        return tickStreamSimulatorDataPath;
    }

    public void setTickStreamSimulatorDataPath(String tickStreamSimulatorDataPath) {
        this.tickStreamSimulatorDataPath = tickStreamSimulatorDataPath;
    }

    public String getLoggingAlertServiceLogFile() {
        return loggingAlertServiceLogFile;
    }

    public void setLoggingAlertServiceLogFile(String loggingAlertServiceLogFile) {
        this.loggingAlertServiceLogFile = loggingAlertServiceLogFile;
    }

    public String getStreamDataStoreRoot() {
        return streamDataStoreRoot;
    }

    public void setStreamDataStoreRoot(String streamDataStoreRoot) {
        this.streamDataStoreRoot = streamDataStoreRoot;
    }

    public String getSubscriberLogin() {
        return subscriberLogin;
    }

    public void setSubscriberLogin(String subscriberLogin) {
        this.subscriberLogin = subscriberLogin;
    }

    public String getSubscriberPassword() {
        return subscriberPassword;
    }

    public void setSubscriberPassword(String subscriberPassword) {
        this.subscriberPassword = subscriberPassword;
    }

    public String getDtnIQFeedClientMode() {
        return (dtnIQFeedClientMode==null) ? "active" : dtnIQFeedClientMode;
    }

    public void setDtnIQFeedClientMode(String dtnIQFeedClientMode) {
        this.dtnIQFeedClientMode = dtnIQFeedClientMode;
    }

    public Integer getLookupThreadCount() {
        return lookupThreadCount;
    }

    public void setLookupThreadCount(int lookupThreadCount) {
        this.lookupThreadCount = lookupThreadCount;
    }

    public int getIdbPerSymbolRetryLimit() {
        return idbPerSymbolRetryLimit;
    }

    public void setIdbPerSymbolRetryLimit(int idbPerSymbolRetryLimit) {
        this.idbPerSymbolRetryLimit = idbPerSymbolRetryLimit;
    }

    public int getIdbRetryLimit() {
        return idbRetryLimit;
    }

    public void setIdbRetryLimit(int idbRetryLimit) {
        this.idbRetryLimit = idbRetryLimit;
    }

    public int getIdbAllowedFailedSymbolsPercent() {
        return idbAllowedFailedSymbolsPercent;
    }

    public void setIdbAllowedFailedSymbolsPercent(int idbAllowedFailedSymbolsPercent) {
        this.idbAllowedFailedSymbolsPercent = idbAllowedFailedSymbolsPercent;
    }

    public long getIdbOverallTimeoutSeconds() {
        return idbOverallTimeoutSeconds;
    }

    public void setIdbOverallTimeoutSeconds(long idbOverallTimeoutSeconds) {
        this.idbOverallTimeoutSeconds = idbOverallTimeoutSeconds;
    }

    public int getSgfPerSymbolRetryLimit() {
        return sgfPerSymbolRetryLimit;
    }

    public void setSgfPerSymbolRetryLimit(int sgfPerSymbolRetryLimit) {
        this.sgfPerSymbolRetryLimit = sgfPerSymbolRetryLimit;
    }

    public int getSgfRetryLimit() {
        return sgfRetryLimit;
    }

    public void setSgfRetryLimit(int sgfRetryLimit) {
        this.sgfRetryLimit = sgfRetryLimit;
    }

    public int getSgfAllowedFailedSymbolsPercent() {
        return sgfAllowedFailedSymbolsPercent;
    }

    public void setSgfAllowedFailedSymbolsPercent(int sgfAllowedFailedSymbolsPercent) {
        this.sgfAllowedFailedSymbolsPercent = sgfAllowedFailedSymbolsPercent;
    }

    public long getSgfOverallTimeoutSeconds() {
        return sgfOverallTimeoutSeconds;
    }

    public void setSgfOverallTimeoutSeconds(long sgfOverallTimeoutSeconds) {
        this.sgfOverallTimeoutSeconds = sgfOverallTimeoutSeconds;
    }
}
