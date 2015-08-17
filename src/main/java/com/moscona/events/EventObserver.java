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

