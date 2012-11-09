package com.datastax.driver.core;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.exceptions.AuthenticationException;

abstract class AbstractReconnectionHandler implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(AbstractReconnectionHandler.class);

    private final ScheduledExecutorService executor;
    private final ReconnectionPolicy policy;
    private final AtomicReference<ScheduledFuture> currentAttempt;

    private volatile boolean readyForNext;
    private volatile ScheduledFuture localFuture;

    public AbstractReconnectionHandler(ScheduledExecutorService executor, ReconnectionPolicy policy, AtomicReference<ScheduledFuture> currentAttempt) {
        this.executor = executor;
        this.policy = policy;
        this.currentAttempt = currentAttempt;
    }

    protected abstract Connection tryReconnect() throws ConnectionException;
    protected abstract void onReconnection(Connection connection);

    protected boolean onConnectionException(ConnectionException e, long nextDelayMs) { return true; }
    protected boolean onUnknownException(Exception e, long nextDelayMs) { return true; }

    // TODO: maybe be shouldn't retry on authentication exception?
    protected boolean onAuthenticationException(AuthenticationException e, long nextDelayMs) { return true; }

    public void start() {
        executor.schedule(this, policy.nextDelayMs(), TimeUnit.MILLISECONDS);

        localFuture = executor.schedule(this, policy.nextDelayMs(), TimeUnit.MILLISECONDS);

        // If there a previous task, cancel it, so only one reconnection handler runs.
        while (true) {
            ScheduledFuture previous = currentAttempt.get();
            if (currentAttempt.compareAndSet(previous, localFuture)) {
                if (previous != null)
                    previous.cancel(false);
                break;
            }
        }
        readyForNext = true;
    }

    public void run() {
        // We shouldn't arrive here if the future is cancelled but better safe than sorry
        if (localFuture.isCancelled())
            return;

        // Don't run before ready, otherwise our cancel business might end up removing all connection attempts.
        while (!readyForNext) {
            try { Thread.sleep(5); } catch (InterruptedException e) {};
        }

        try {
            onReconnection(tryReconnect());
            currentAttempt.compareAndSet(localFuture, null);
        } catch (ConnectionException e) {
            long nextDelay = policy.nextDelayMs();
            if (onConnectionException(e, nextDelay))
                reschedule(nextDelay);
            else
                currentAttempt.compareAndSet(localFuture, null);
        } catch (AuthenticationException e) {
            logger.error(e.getMessage());
            long nextDelay = policy.nextDelayMs();
            if (onAuthenticationException(e, nextDelay))
                reschedule(nextDelay);
            else
                currentAttempt.compareAndSet(localFuture, null);
        } catch (Exception e) {
            long nextDelay = policy.nextDelayMs();
            if (onUnknownException(e, nextDelay))
                reschedule(nextDelay);
            else
                currentAttempt.compareAndSet(localFuture, null);
        }
    }

    private void reschedule(long nextDelay) {
        readyForNext = false;
        ScheduledFuture newFuture = executor.schedule(this, nextDelay, TimeUnit.MILLISECONDS);
        assert localFuture != null;
        // If it's not our future the current one, then we've been canceled
        if (!currentAttempt.compareAndSet(localFuture, newFuture)) {
            newFuture.cancel(false);
        }
        localFuture = newFuture;
        readyForNext = true;
    }
}
