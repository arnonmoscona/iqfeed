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
