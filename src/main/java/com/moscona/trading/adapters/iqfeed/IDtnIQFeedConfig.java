package com.moscona.trading.adapters.iqfeed;

import com.moscona.events.EventPublisher;
import com.moscona.exceptions.InvalidArgumentException;
import com.moscona.exceptions.InvalidStateException;
import com.moscona.trading.IServiceBundleManagerConfig;
import com.moscona.trading.IServicesBundle;
import com.moscona.trading.ServicesBundle;
import com.moscona.trading.formats.deprecated.MarketTree;
import com.moscona.trading.formats.deprecated.MasterTree;
import com.moscona.util.IAlertService;
import com.moscona.util.monitoring.stats.IStatsService;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.HashMap;

/**
 * Configuration data for IQFeed adapters
 * Created: 2/13/14 8:06 AM
 * By: Arnon Moscona
 */
public interface IDtnIQFeedConfig extends IServiceBundleManagerConfig {
    /**
     * A convenience service method that returns the component specific configuration info
     * @param component the name of the component
     * @return the entry in the component config hash (it's up to the component to determine validity of this
     */
    public Object getComponentConfigFor(String component); // FIXME does not make sense in the context of only IQFeed, as opposed to server

    public HashMap getComponentConfig();

    public String simpleComponentConfigEntryWithOverride(String componentNode, String key, String overrideProperty) throws InvalidArgumentException, InvalidStateException, IOException;

    public String simpleComponentConfigEntryWithOverride(String componentNode, String key) throws InvalidArgumentException, InvalidStateException, IOException;

    public ServicesBundle getServicesBundle() throws InvalidArgumentException, InvalidStateException;

    public IAlertService getAlertService() throws InvalidStateException, InvalidArgumentException;

    public IStatsService getStatsService() throws InvalidStateException, InvalidArgumentException;

    public IServicesBundle createServicesBundle() throws InvalidStateException, InvalidArgumentException;

    public EventPublisher getEventPublisher(); // FIXME remove dependency on EventPublisher

    public IStatsService getLookupStatsService();

    public String getStreamDataStoreRoot() throws IOException, InvalidStateException, InvalidArgumentException; // FIXME replace with proper injector of a proper logger

    public LogFactory getLogFactory();

    public MarketTree getMarketTree(); // FIXME get rid of dependency on MarketTree
    public MasterTree getMasterTree(); // FIXME get rid of dependency on MarketTree
}

