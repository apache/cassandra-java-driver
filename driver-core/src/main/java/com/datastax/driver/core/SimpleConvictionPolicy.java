package com.datastax.driver.core;

public class SimpleConvictionPolicy implements ConvictionPolicy {

    private final Host host;

    private SimpleConvictionPolicy(Host host) {
        this.host = host;
    }

    public boolean addFailure(ConnectionException exception) {
        // TODO: be kinder
        return true;
    }

    public boolean addFailureFromExternalDetector() {
        return true;
    }

    public void reset() {}

    public static class Factory implements ConvictionPolicy.Factory {

        public ConvictionPolicy create(Host host) {
            return new SimpleConvictionPolicy(host);
        }
    }
}
