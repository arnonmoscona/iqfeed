package com.moscona.trading.adapters.iqfeed;

import com.moscona.exceptions.InvalidArgumentException;
import com.moscona.exceptions.InvalidStateException;
import com.moscona.trading.ServicesBundle;
import com.moscona.util.IAlertService;
import com.moscona.util.IStatsService;

/**
 * Configuration data for IQFeed adapters
 * Created: 2/13/14 8:06 AM
 * By: Arnon Moscona
 */
public interface IDtnIQFeedConfig {
    /**
     * A convenience service method that returns the component specific configuration info
     * @param component the name of the component
     * @return the entry in the component config hash (it's up to the component to determine validity of this
     */
    Object getComponentConfigFor(String component); // FIXME does not ake sense in the context of only IQFeed, as opposed to server

    public String simpleComponentConfigEntryWithOverride(String componentNode, String key, String overrideProperty) throws InvalidArgumentException, InvalidStateException;

    public String simpleComponentConfigEntryWithOverride(String componentNode, String key) throws InvalidArgumentException, InvalidStateException;

    public ServicesBundle getServicesBundle();

    public IAlertService getAlertService();

    public IStatsService getStatsService();

}
