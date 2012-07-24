package com.datastax.driver.core;

import java.net.InetSocketAddress;

import com.datastax.driver.core.pool.HealthMonitor;
import com.datastax.driver.core.transport.ConnectionException;

/**
 * A Cassandra node.
 */
public class Host {

    private final InetSocketAddress address;
    private final HealthMonitor monitor;

    Host(InetSocketAddress address, ConvictionPolicy.Factory policy) {
        if (address == null || policy == null)
            throw new NullPointerException();

        this.address = address;
        this.monitor = new HealthMonitor(this, policy.create(this));
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public HealthMonitor monitor() {
        return monitor;
    }

    @Override
    public final int hashCode() {
        return address.hashCode();
    }

    @Override
    public final boolean equals(Object o) {
        if(!(o instanceof Host))
            return false;

        return address.equals(((Host)o).address);
    }

    @Override
    public String toString() {
        return address.toString();
    }

    // TODO: see if we can make that package protected (we should at leat not
    // make the HealthMonitor interface public itself, but only the
    // AbstractHealthMonitor abstract methods). The rational being that we don't
    // want user to call onUp and onDown themselves.
    public interface StateListener {

        public void onUp(Host host);
        public void onDown(Host host);

    }
}
