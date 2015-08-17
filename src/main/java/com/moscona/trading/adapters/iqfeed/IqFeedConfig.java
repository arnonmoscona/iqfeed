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

import com.moscona.events.EventPublisher;
import com.moscona.exceptions.InvalidArgumentException;
import com.moscona.exceptions.InvalidStateException;
import com.moscona.test.util.TestResourceHelper;
import com.moscona.trading.IServicesBundle;
import com.moscona.trading.IServicesBundleManager;
import com.moscona.trading.ServicesBundle;
import com.moscona.trading.ServicesBundleManager;
import com.moscona.trading.formats.deprecated.MarketTree;
import com.moscona.trading.formats.deprecated.MasterTree;
import com.moscona.util.IAlertService;
import com.moscona.util.monitoring.stats.IStatsService;
import com.moscona.util.monitoring.stats.SimpleStatsService;
import com.moscona.util.monitoring.stats.SynchronizedDelegatingStatsService;
import org.apache.commons.beanutils.BeanMap;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Arnon on 5/13/2014.
 * This is a temporary bridging solution. A subset of the old ServerConfig, this class implements the base minimum to
 * pass the relevant EasyB test.
 * It should be refactored heavily to separate between the loadable configuration, the dependency injection and
 * factory method aspects, and the generic service method aspects.
 * FIXME separate the loadable config aspects from factory methods, dependency injection, plugin support
 */
public class IqFeedConfig implements IDtnIQFeedConfig {
    private final boolean configFullyLoaded;
    /**
     * Configuration parameters for components that need additional configuration
     */
    private HashMap componentConfig;

    /**
     * A list of class names keyed by their associated attributes that are initialized as plugin components at the very
     * end of the load sequence
     * FIXME may not be needed
     */
    private HashMap<String, String> pluginComponents;

    private UserOverrides userOverrides;

    /**
     * The default servicesBundle bundle to use
     */
    private ServicesBundle servicesBundle;

    private IServicesBundleManager servicesBundleManager;


    private TestResourceHelper testResourceHelper = null;
    private String alertServiceClassName; // FIXME should be proper dependency injection
    private String statsServiceClassName; // FIXME should be proper dependency injection
    private EventPublisher eventPublisher;  // FIXME Should replace EventPublisher with something cleaner
    private SynchronizedDelegatingStatsService lookupStatsService;
    private String streamDataStoreRoot; // FIXME rename. I think it's used for logging root only
    private LogFactory logFactory;
    private MarketTree marketTree; // FIXME get rid of dependency on MarketTree
    private MasterTree masterTree; // FIXME get rid of dependency on MarketTree


    public IqFeedConfig() {
        configFullyLoaded = false;  // FIXME implement the entire plugin loading and configuration cycle
        eventPublisher = new EventPublisher();
        eventPublisher.clearSubscribers();
    }


    /**
     * A convenience service method that returns the component specific configuration info
     * @param component the name of the component
     * @return the entry in the component config hash (it's up to the component to determine validity of this
     */
    @Override
    public Object getComponentConfigFor(String component) {
        return componentConfig.get(component);
    }

    /**
     * A utility methods to make it easier for components to configure themselves using the componentConfig
     * configuration and user overrides. It covers the most common case, where the configuration is a HashMap
     * that maps String to String and the override property is a string as well.
     * @param componentNode the key in the componentConfig hash for this component
     * @param key the key in the component node for the configuration item
     * @param overrideProperty the property name in the UserOverrides class. May be null. If not must exist.
     * @return the value, with the override applied if the override exists and is not blank
     * @throws InvalidArgumentException
     */
    @Override
    public String simpleComponentConfigEntryWithOverride(String componentNode, String key, String overrideProperty) throws InvalidArgumentException, InvalidStateException, IOException {
        String retval = null;
        try {
            HashMap componentConfig = (HashMap) getComponentConfigFor(componentNode);
            if (componentConfig != null) {
                retval = componentConfig.get(key).toString();
            }
            if (userOverrides != null && overrideProperty != null) {
                BeanMap overrides = new BeanMap(userOverrides);
                if (overrides.containsKey(overrideProperty)) {
                    Object value = overrides.get(overrideProperty);
                    String overrideValue = (value == null) ? null : value.toString();
                    if (!StringUtils.isBlank(overrideValue)) {
                        retval = overrideValue;
                    }
                }
            }
        } catch (ClassCastException e) {
            throw new InvalidArgumentException("Class cast problem in simpleComponentConfigEntryWithOverride() - most likely a mismatch between waht you expect the config or the overrides to contain and what they actually contain: " + e, e);
        }
        retval = interpolate(retval);
        return retval;
    }

    @Override
    public String simpleComponentConfigEntryWithOverride(String componentNode, String key) throws InvalidArgumentException, InvalidStateException, IOException {
        return simpleComponentConfigEntryWithOverride(componentNode, key, key);
    }

    @Override
    public ServicesBundle getServicesBundle() throws InvalidArgumentException, InvalidStateException {
        if (servicesBundle == null) {
            servicesBundle = getServicesBundleManager().createServicesBundle();
        }
        return servicesBundle;
    }

    public void setServicesBundle(ServicesBundle servicesBundle) {
        this.servicesBundle = servicesBundle;
    }


    public void initServicesBundle() throws InvalidArgumentException, InvalidStateException {
        servicesBundleManager = new ServicesBundleManager(this);
        servicesBundle = servicesBundleManager.createServicesBundle();
    }


    @Override
    public IAlertService getAlertService() throws InvalidStateException, InvalidArgumentException {
        return getServicesBundle().getAlertService();
    }

    public void setAlertService(IAlertService alertService) {
        if (servicesBundle == null) {
            servicesBundle = new ServicesBundle();
        }
        servicesBundle.setAlertService(alertService);
    }

    @Override
    public IStatsService getStatsService() throws InvalidStateException, InvalidArgumentException {
        return getServicesBundle().getStatsService();
    }

    public void setStatsService(IStatsService statsService) {
        if (servicesBundle == null) {
            servicesBundle = new ServicesBundle();
        }
        servicesBundle.setStatsService(statsService);
    }

    /**
     * A factory method to create services bundles compatible with the configuration. Used to create the default
     * services bundle but also for any code that needs to create private instances, predominantly required for
     * anything that runs in a separate thread.
     * @return a new instance of a a services bundle
     * @throws com.moscona.exceptions.InvalidStateException if cannot construct the appropriate classes
     */
    @Override
    public ServicesBundle createServicesBundle() throws InvalidStateException, InvalidArgumentException {
        return getServicesBundleManager().createServicesBundle();
    }

    private synchronized IServicesBundleManager getServicesBundleManager() throws InvalidArgumentException {
        if (servicesBundleManager == null) {
            servicesBundleManager = new ServicesBundleManager<IDtnIQFeedConfig>(this);
        }
        return servicesBundleManager;
    }

    @Override
    public EventPublisher getEventPublisher() {
        return eventPublisher;
    }


    @Override
    public synchronized IStatsService getLookupStatsService() {
        if (lookupStatsService == null) {
            lookupStatsService = new SynchronizedDelegatingStatsService(new SimpleStatsService());
        }
        return lookupStatsService;
    }

    @Override
    public String getStreamDataStoreRoot() throws InvalidStateException, InvalidArgumentException, IOException {
        // FIXME Rename this. I think it's used only as a logging root in this context. When renaming need to change YAML fixtures
        String storeRoot = streamDataStoreRoot;
        if (userOverrides != null && !StringUtils.isBlank(userOverrides.getStreamDataStoreRoot())) {
            storeRoot = userOverrides.getStreamDataStoreRoot();
        }
        return interpolate(storeRoot);
    }

    public void setStreamDataStoreRoot(String streamDataStoreRoot) {
        this.streamDataStoreRoot = streamDataStoreRoot;
    }

    @Override
    public HashMap getComponentConfig() {
        return componentConfig;
    }

    public void setComponentConfig(HashMap componentConfig) {
        this.componentConfig = componentConfig;
    }


    /**
     * Interpolates strings used in configuration like: "#SystemProperty{user.home}/intellitrade_alerts.log".
     * The key pattern is #type{value} - the type being a supported type of interpolation and the value is the argument
     * for this interpolation.
     * <br/>Supported types:
     * <ul>
     * <li>SystemProperty - a value from the java system properties</li>
     * <li>ProjectRoot - the IntelliJ IDEA project root - used only in testing context</li>
     * </ul>
     * @param arg the string to interpolate
     * @return a new string with all the possible interpolation expanded and the original "macro" parts removed
     * @throws InvalidArgumentException is you try to use an interpolation type that is not supported
     * @throws com.moscona.exceptions.InvalidStateException if happens below
     */
    public String interpolate(String arg) throws InvalidArgumentException, InvalidStateException, IOException {
        // todo need spec
        String retval = arg;
        Pattern pattern = Pattern.compile("(.*)#([a-zA-Z]+)\\{(.*)\\}(.*)");
        Matcher matcher = pattern.matcher(arg);
        int maxIterations = 100;

        while (matcher.matches()) {
            String interpolationType = matcher.group(2);
            if (interpolationType.equals("SystemProperty")) {
                String property = matcher.group(3);
                String value = System.getProperty(property);
                if (value == null) {
                    throw new InvalidArgumentException("ServerConfig.interpolate(): System property \"" + property + "\" does not exist when interpolating \"" + arg + "\"");
                }
                retval = matcher.group(1) + value + matcher.group(4);
            } else if (interpolationType.equals("ProjectRoot")) {
                // FIXME this whole section is suspicious and should be probably removed
//                String classPath = "/com/intellitrade/server/ServerConfig.class";
//                String outputPath = "/out/production/server" + classPath;
//                String value = ServerConfig.class.getResource(classPath).toString().replaceAll(outputPath, "").replaceAll("file:/", "");
//                retval = matcher.group(1) + value + matcher.group(4);
                retval = getTestResourceHelper().getProjectRootPath();
            } else if (interpolationType.equals("streamDataStoreRoot")) {
                retval = matcher.group(1) + getStreamDataStoreRoot() + matcher.group(4);
            } else {
                throw new InvalidArgumentException("ServerConfig.interpolate() does not support interpolation type " + interpolationType + " in the string: \"" + arg + "\"");
            }

            matcher = pattern.matcher(retval);
            if (maxIterations-- <= 0) {
                throw new InvalidStateException("ServerConfig.interpolate() iterated too many times without resolving \"" + arg + "\"");
            }
        }

        return retval;
    }

    private synchronized TestResourceHelper getTestResourceHelper() throws IOException {
        if (testResourceHelper == null) {
            testResourceHelper = new TestResourceHelper();
        }
        return testResourceHelper;
    }

    @Override
    public String getAlertServiceClassName() {
        return alertServiceClassName;
    }

    public void setAlertServiceClassName(String alertServiceClassName) {
        this.alertServiceClassName = alertServiceClassName;
    }

    @Override
    public String getStatsServiceClassName() {
        return statsServiceClassName;
    }

    public void setStatsServiceClassName(String statsServiceClassName) {
        this.statsServiceClassName = statsServiceClassName;
    }

    @Override
    public LogFactory getLogFactory() {
        return logFactory;
    }

    public void setLogFactory(LogFactory logFactory) {
        this.logFactory = logFactory;
    }

    @Override
    public MarketTree getMarketTree() {  // FIXME get rid of this method
        return marketTree;
    }

    public void setMarketTree(MarketTree marketTree) {  // FIXME get rid of this method
        this.marketTree = marketTree;
    }

    @Override
    public MasterTree getMasterTree() {  // FIXME get rid of this method
        return masterTree;
    }

    public void setMasterTree(MasterTree masterTree) {    // FIXME get rid of this method
        this.masterTree = masterTree;
    }
}
