package com.datastax.driver.core;

import com.datastax.driver.core.transport.ConnectionException;

public interface ConvictionPolicy {

    // Return wether to mark the node down
    public boolean addFailure(ConnectionException exception);

    // Return wether to mark the node down
    public boolean addFailureFromExternalDetector();

    public interface Factory {
        public ConvictionPolicy create(Host host);
    }
}
