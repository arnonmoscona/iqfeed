package com.moscona.events;

import com.moscona.util.SafeRunnable;

import java.util.Observable;
import java.util.Observer;

/**
 * Created: May 3, 2010 12:54:34 PM
 * By: Arnon Moscona
 * A an adapter that makes safe runnables into Observers
 */
public class EventObserver implements Observer {
    private SafeRunnable action;
    private String eventName="unknown";

    public EventObserver(SafeRunnable action, String forEvent) {
        this.action = action;
        this.eventName = forEvent;
    }

    /**
     * This method is called whenever the observed object is changed. An
     * application calls an <tt>Observable</tt> object's
     * <code>notifyObservers</code> method to have all the object's
     * observers notified of the change.
     *
     * @param o   the observable object.
     * @param arg an argument passed to the <code>notifyObservers</code>
     *            method.
     */
    @Override
    public void update(Observable o, Object arg) {
        if (EventPublisher.isInDebugMode()) {
            System.out.println("==> running an action '"+action.getName() + "' for "+eventName);
        }
        action.run();
    }
}

