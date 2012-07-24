package com.datastax.driver.core;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.transport.Connection;
import com.datastax.driver.core.transport.ConnectionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReconnectionHandler implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ReconnectionHandler.class);

    private final Host host;
    private final ScheduledExecutorService executor;
    private final Connection.Factory factory;

    // The next delay in milliseconds
    // TODO: implements something better than "every 3 seconds"
    private int nextDelay = 3000;

    public ReconnectionHandler(Host host, ScheduledExecutorService executor, Connection.Factory factory) {
        this.host = host;
        this.executor = executor;
        this.factory = factory;
    }

    public void start() {
        executor.schedule(this, nextDelay, TimeUnit.MILLISECONDS);
    }

    public void run() {
        try {
            factory.open(host);
            // If we're successful, the node is up and ready
            logger.debug(String.format("Successful connection to %s, setting host UP", host));
            host.monitor().reset();
        } catch (ConnectionException e) {
            // TODO: log the failure and implement some better policy of retry
            scheduleRetry();
        } catch (Exception e) {
            // TODO: log that something is wrong
            scheduleRetry();
        }
    }

    private void scheduleRetry() {
        logger.debug(String.format("Failed connection to %s, scheduling retry in %d milliseconds", host, nextDelay));
        executor.schedule(this, nextDelay, TimeUnit.MILLISECONDS);
    }
}
