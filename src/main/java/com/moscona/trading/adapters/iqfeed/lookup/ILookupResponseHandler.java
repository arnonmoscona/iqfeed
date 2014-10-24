package com.moscona.trading.adapters.iqfeed.lookup;

/**
 * Created by arnon on 10/2/2014.
 * A handler for lookup response.
 * Unfortunately there is no clean candidaye in Java8 functional interfaces.
 * It's close to an Action, but does not extend ActionListener and has nothing to do with GUI
 */
public interface ILookupResponseHandler<T> {
    public void handleResponse(T response);
}
