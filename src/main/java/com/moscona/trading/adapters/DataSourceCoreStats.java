package com.moscona.trading.adapters;

import com.moscona.events.EventPublisher;
import com.moscona.util.StringHelper;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * Created: Aug 17, 2010 2:55:39 PM
 * By: Arnon Moscona
 * Core monitoring statistics of the external real time data source (not generic statistics for things like JMX)
 */
@SuppressWarnings({"InstanceVariableMayNotBeInitialized", "AssignmentToDateFieldFromParameter"})
public class DataSourceCoreStats implements Cloneable {
    private long totalStaleTicks;
    private float avgStaleness;
    private float percentStaleTicks;
    private int lastStaleTickAge;
    private int rawMessageQueueLength;
    private long outOfOrderTicks;
    private int timeMessageLatency;
    public static final String DATA_SOURCE_CORE_STATS = "DataSourceCoreStats";
    private int tickQueueSize;
    private float bwConsumption;
    private String lastStaleTickSymbol;
    private Calendar lastStaleTickTimeStamp;
    private int lastHiccupDuration;
    private Calendar lastHiccupTimeStamp;
    private String lastOutOfOrderTickSymbol;

    public DataSourceCoreStats() {

    }

    public DataSourceCoreStats(float avgStaleness, int lastStaleTickAge, long outOfOrderTicks, float percentStaleTicks,
                               int rawMessageQueueLength, int timeMessageLatency, long totalStaleTicks,
                               int tickQueueSize, float bwConsumption, String lastStaleTickSymbol,
                               Calendar lastStaleTickTimeStamp, int lastHiccupDuration, Calendar lastHiccupTimeStamp,
                               String lastOutOfOrderTickSymbol) {
        this.avgStaleness = avgStaleness;
        this.lastStaleTickAge = lastStaleTickAge;
        this.outOfOrderTicks = outOfOrderTicks;
        this.percentStaleTicks = percentStaleTicks;
        this.rawMessageQueueLength = rawMessageQueueLength;
        this.timeMessageLatency = timeMessageLatency;
        this.totalStaleTicks = totalStaleTicks;
        this.tickQueueSize = tickQueueSize;
        this.bwConsumption = bwConsumption;
        this.lastStaleTickSymbol = lastStaleTickSymbol;
        this.lastHiccupDuration = lastHiccupDuration;
        this.lastHiccupTimeStamp = lastHiccupTimeStamp;
        this.lastStaleTickTimeStamp = lastStaleTickTimeStamp;
        this.lastOutOfOrderTickSymbol = lastOutOfOrderTickSymbol;
    }

    @Override
    public DataSourceCoreStats clone() {
        DataSourceCoreStats retval = new DataSourceCoreStats();
        try {
            retval = (DataSourceCoreStats)super.clone();
        }
        catch (Exception e) {
            // ignore (won't happen. Heh, asking for trouble :-)
        }
        return retval;
    }

    public float getAvgStaleness() {
        return avgStaleness;
    }

    public int getLastStaleTickAge() {
        return lastStaleTickAge;
    }

    public long getOutOfOrderTicks() {
        return outOfOrderTicks;
    }

    public float getPercentStaleTicks() {
        return percentStaleTicks;
    }

    public int getRawMessageQueueLength() {
        return rawMessageQueueLength;
    }

    public int getTimeMessageLatency() {
        return timeMessageLatency;
    }

    public long getTotalStaleTicks() {
        return totalStaleTicks;
    }

    public float getBwConsumption() {
        return bwConsumption;
    }

    public int getTickQueueSize() {
        return tickQueueSize;
    }

    public String getLastStaleTickSymbol() {
        return lastStaleTickSymbol;
    }

    public int getLastHiccupDuration() {
        return lastHiccupDuration;
    }

    public Calendar getLastHiccupTimeStamp() {
        return lastHiccupTimeStamp;
    }

    public Calendar getLastStaleTickTimeStamp() {
        return lastStaleTickTimeStamp;
    }

    public String getLastOutOfOrderTickSymbol() {
        return lastOutOfOrderTickSymbol;
    }

    public static DataSourceCoreStats extract(Map<String, Object> metadata) {
        if (metadata.containsKey(DATA_SOURCE_CORE_STATS)) {
            return (DataSourceCoreStats)metadata.get(DATA_SOURCE_CORE_STATS);
        }
        return null;
    }

    public void putIn(Map<String, Object> metadata) {
        metadata.put(DATA_SOURCE_CORE_STATS, this);
    }

    @Override
    public String toString() {
        return StringHelper.toString(this);
    }

    public void publish(EventPublisher publisher) {
        HashMap<String,Object> metadata = new HashMap<String,Object>();
        putIn(metadata);
        publisher.publish(EventPublisher.Event.STATS_UPDATE_RESPONSE,metadata);
    }
}
