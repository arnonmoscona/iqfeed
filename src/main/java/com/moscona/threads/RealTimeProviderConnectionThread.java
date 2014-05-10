package com.moscona.threads;

/**
 * Created: Jul 5, 2010 11:36:15 AM
 * By: Arnon Moscona
 */
public class RealTimeProviderConnectionThread<C> extends ServerThread<C> {
    public RealTimeProviderConnectionThread(Runnable code, String name, C config) {
        super(code,"real time connection - "+name,config);
    }
}

