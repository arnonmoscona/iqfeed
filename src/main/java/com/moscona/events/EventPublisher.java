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

package com.moscona.events;

import com.moscona.exceptions.InvalidArgumentException;
import com.moscona.util.SafeRunnable;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Observable;

// FIXME This class is old and is not a particularly good event model. Should be replaced by EventBus, MBassador, or something similar

/**
 * Created: May 3, 2010 12:35:50 PM
 * By: Arnon Moscona
 * A class that is responsible for managing and publishing named events to observers.
 * The class only works with event types that are known at compile time. However their subjects may vary.
 * Note that events can be published and intercepted in any thread and that this class makes no provisions
 * for thread safety.
 * Also note that some of the event publishers do it in real time classes (although not in the middle of real
 * time events) - and so listeners should return quickly. If they have anything long to do then they should do it in
 * another thread.
 * Finally note that this facility is intended to be used for low frequency system events, and not for high frequency
 * market events.
 */
public class EventPublisher {
    private static boolean debug = false;

    /**
     * The events that clients can work with
     */
    public static enum Event {
        FATAL_ERROR_RESTART("FATAL_ERROR_RESTART"),
        TRADING_OPENED("TRADING_OPENED"),
        TRADING_CLOSED("TRADING_CLOSED"),
        SERVER_STARTING("SERVER_STARTING"), // before server started - allows components to set themselves up if they need to delay server start
        SERVER_START("SERVER_START"),
        SERVER_SHUTDOWN_IN_PROGRESS("SERVER_SHUTDOWN_IN_PROGRESS"),
        SERVER_SHUTDOWN_COMPLETE("SERVER_SHUTDOWN_COMPLETE"),
        SERVER_ALERT("SERVER_ALERT"),
        PREPARE_FOR_TRADING_DAY_OPEN("PREPARE_FOR_TRADING_DAY_OPEN"), // fired before the trading day to ensure that all preparations are done
        TASK_STARTED("TASK_STARTED"), // fired when tasks that later will send progress updates via TASK_PROGRESS start. Uses "taskName" in metadata to denote which task.
        TASK_FINISHED("TASK_FINISHED"), // fired when tasks that later will send progress updates via TASK_PROGRESS start. Uses "taskName" in metadata to denote which task.
        TASK_PROGRESS("TASK_PROGRESS"), // fired when tasks make progress that some progress indicator may (or may not) show. Uses metadata compatible with ProgressEventMetadata
        STATUS_UPDATE("STATUS_UPDATE"), // fired when components want to notify the UI about status updates
        INSPECTION_START("INSPECTION_START"), // fired when an inspection process starts
        INSPECTION_UPDATE("INSPECTION_UPDATE"), // fired when an inspection process wants to send a cumulative update for some results
        INSPECTION_END("INSPECTION_END"), // fired when an inspection process ends
        NEW_MARKET_TREE_AVAILABLE("NEW_MARKET_TREE_AVAILABLE"), // fired when a new market tree file has been generated. Metadata: "path"
        DEBUG_GAP_INJECTION_ENABLE_DISABLE("DEBUG_GAP_INJECTION_ENABLE_DISABLE"), // fired when the gui want to enable or disable gap injection
        DEBUG_GAP_INJECTION_SKIPPED_SNAPSHOT("DEBUG_GAP_INJECTION_SKIPPED_SNAPSHOT"), // fired when the snapshot writer injects a skipped snapshot
        STATS_UPDATE_REQUEST("STATS_UPDATE_REQUEST"), // fired when someone wants to collect stats from everybody who knows how to publish stats (generic or core)
        STALE_STATS_RESET_REQUEST("STALE_STATS_RESET_REQUEST"), // fired when someone wants to reset the stale stats (facilitates GUI control)
        STATS_UPDATE_RESPONSE("STATS_UPDATE_REQUEST"), // fired when a component wants to publish a stat. Use a map manufactured with one of the stats map factories here
        DUMP_MEMORY_TRACKER_REQUEST("DUMP_MEMORY_TRACKER_REQUEST"); // a request to dump active MemoryStateHistory data to a file. Generally by manual UI action

        private final MyObservable observable;
        private Map<String, Object> transientMetadata = null;
        private String name;

        Event(String name) {
            observable = new MyObservable();
            this.name = name;
        }

        public void trigger() {
            observable.setChanged();
            observable.notifyObservers();
        }

        public void addAction(EventObserver action) {
            observable.addObserver(action);
        }

        public void clearSubscribers() {
            observable.deleteObservers();
        }

        /**
         * Attaches transient metadata to the event
         * @param transientMetadata the metadata to attach
         */
        public synchronized void attachMetadata(Map<String, Object> transientMetadata) {
            this.transientMetadata = transientMetadata;
        }

        public synchronized Map<String, Object> getMetadata() {
            return Collections.unmodifiableMap(transientMetadata);
        }

        public String getName() {
            return name;
        }
    }

    public EventPublisher() {
        // todo implement constructor
    }

    public void onEvent(Event event, EventObserver action) {
        event.addAction(action);
    }

    public void onEvent(Event event, SafeRunnable action) {
        if (debug) {
            System.out.println(">> onEvent("+event.getName()+") run action: "+action.getName());
        }
        event.addAction(new EventObserver(action, event.getName()));
    }

    public void publish(Event event) {
        publish(event, null);
    }

    /**
     * Allows publishing an event with attached metadata. The metadata is transient and the contract is that it will
     * stay around (but may be modified by subscribers) until wither all subscribers have been notified *or* until
     * another publish call happens - whichever comes first.
     * @param event the event to be published
     * @param transientMetadata the attached metadata
     */
    public void publish(Event event, Map<String, Object> transientMetadata) {
        event.attachMetadata(transientMetadata);
        event.trigger();
    }

    public void clearSubscribers() {
        for (Event event: Event.values()) {
            event.clearSubscribers();
        }
    }

    /**
     * just used to expose setChanged()
     */
    private static class MyObservable extends Observable {
        @Override
        public void setChanged() {
            super.setChanged();
        }
    }

    protected static boolean isInDebugMode() {
        return debug;
    }

    public void publishStatusUpdate(String item, String status) throws InvalidArgumentException {
        if (item==null) {
            throw new InvalidArgumentException("Item may not be null in publishStatusUpdate()");
        }
        HashMap<String,Object> metadata = new HashMap<String,Object>();
        metadata.put("item", item);
        metadata.put("status", status);
        publish(Event.STATUS_UPDATE,metadata);
    }

    public void publishInspectionStart(String name) throws InvalidArgumentException {
        HashMap<String,Object> metadata = new HashMap<String,Object>();
        fillInspectionName(name, metadata);
        publish(Event.INSPECTION_START, metadata);
    }

    public void publishInspectionEnd(String name) throws InvalidArgumentException {
        HashMap<String,Object> metadata = new HashMap<String,Object>();
        fillInspectionName(name, metadata);
        publish(Event.INSPECTION_END, metadata);
    }

    public void publishInspectionUpdate(String name, String subject, String description, String outcomeClass) throws InvalidArgumentException {
        HashMap<String,Object> metadata = new HashMap<String,Object>();
        fillInspectionName(name, metadata);
        fillInField("subject", "Subject of inspection", subject, true, metadata);
        fillInField("description", "Description of outcome for "+subject, description, true, metadata);
        fillInField("outcomeClass", "Outcome class for "+subject+" (e.g. severe)", outcomeClass, true, metadata);
        publish(Event.INSPECTION_UPDATE, metadata);
    }

    private void fillInspectionName(String name, HashMap<String, Object> metadata) throws InvalidArgumentException {
        fillInField("name", "Inspection name", name, true, metadata);
    }

    private void fillInField(String field, String desc, String value, boolean required, HashMap<String, Object> metadata) throws InvalidArgumentException {
        if (required && StringUtils.isBlank(value)) {
            throw new InvalidArgumentException(desc+" is required");
        }
        metadata.put(field, value);
    }


    public void publishProblem(String message) throws InvalidArgumentException {
        publishStatusUpdate("problems",message);
    }

    public void publishDebugGapInjectionRequest(Boolean enable, Integer gapLength, Integer gapProbability) {
        HashMap<String,Object> metadata = new HashMap<String,Object>();
        metadata.put("enable",enable);
        metadata.put("gapLength", gapLength);
        metadata.put("gapProbability", gapProbability);
        publish(Event.DEBUG_GAP_INJECTION_ENABLE_DISABLE,metadata);
    }

    public void publishDebugGapInjected(String filename) {
        HashMap<String,Object> metadata = new HashMap<String,Object>();
        metadata.put("filename",filename);
        publish(Event.DEBUG_GAP_INJECTION_SKIPPED_SNAPSHOT,metadata);
    }
}
