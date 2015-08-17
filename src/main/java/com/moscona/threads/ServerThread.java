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

package com.moscona.threads;

import com.moscona.util.concurrent.DaemonThread;

/**
 * Created: Jul 5, 2010 11:33:44 AM
 * By: Arnon Moscona
 */
public class ServerThread<C> extends DaemonThread {
    protected C config;
    private boolean safeToKill = false;

    public ServerThread(Runnable code, String name, C config) {
        super(code,"IntelliTrade Server - "+name);
        this.config = config;
    }

    /**
     * A variant of the constructor that allows flagging this instance as safe to kill
     * @param code
     * @param name
     * @param config
     * @param safeToKill if true, then the server thread will be killed if it becomes a zombie
     */
    public ServerThread(Runnable code, String name, C config, boolean safeToKill) {
        this(code,name,config);
        this.safeToKill = safeToKill;
    }

    public boolean isSafeToKill() {
        return safeToKill;
    }
}
