package com.datastax.driver.core.pool;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import com.datastax.driver.core.ConvictionPolicy;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.transport.ConnectionException;

public class HealthMonitor {

    private final Host host;
    private final ConvictionPolicy policy;

    private Set<Host.StateListener> listeners = new CopyOnWriteArraySet<Host.StateListener>();
    private volatile boolean isUp;

    public HealthMonitor(Host host, ConvictionPolicy policy) {
        this.host = host;
        this.isUp = true;
        this.policy = policy;
    }

    public void register(Host.StateListener listener) {
        listeners.add(listener);
    }

    public void unregister(Host.StateListener listener) {
        listeners.add(listener);
    }

    public boolean isUp() {
        return isUp;
    }

    boolean signalConnectionFailure(ConnectionException exception) {
        boolean isDown = policy.addFailure(exception);
        if (isDown)
            setDown();
        return isDown;
    }

    // TODO: Should we bother making sure that multiple calls to this don't inform the listeners twice?
    private void setDown() {
        isUp = false;
        for (Host.StateListener listener : listeners)
            listener.onDown(host);
    }

    public void reset() {
        isUp = true;
        for (Host.StateListener listener : listeners)
            listener.onUp(host);
    }
}
