/*
 *      Copyright (C) 2012 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.*;

import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.exceptions.AuthenticationException;

abstract class AbstractReconnectionHandler implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(AbstractReconnectionHandler.class);

    private final ScheduledExecutorService executor;
    private final ReconnectionPolicy.ReconnectionSchedule schedule;
    private final AtomicReference<ScheduledFuture<?>> currentAttempt;

    private volatile boolean readyForNext;
    private volatile ScheduledFuture<?> localFuture;

    public AbstractReconnectionHandler(ScheduledExecutorService executor, ReconnectionPolicy.ReconnectionSchedule schedule, AtomicReference<ScheduledFuture<?>> currentAttempt) {
        this.executor = executor;
        this.schedule = schedule;
        this.currentAttempt = currentAttempt;
    }

    protected abstract Connection tryReconnect() throws ConnectionException, InterruptedException;
    protected abstract void onReconnection(Connection connection);

    protected boolean onConnectionException(ConnectionException e, long nextDelayMs) { return true; }
    protected boolean onUnknownException(Exception e, long nextDelayMs) { return true; }

    // Retrying on authentication error is unlikely to work
    protected boolean onAuthenticationException(AuthenticationException e, long nextDelayMs) { return false; }

    public void start() {
        long firstDelay = schedule.nextDelayMs();
        logger.debug("First reconnection scheduled in {}ms", firstDelay);
        try {
            localFuture = executor.schedule(this, firstDelay, TimeUnit.MILLISECONDS);

            // If there a previous task, cancel it, so only one reconnection handler runs.
            while (true) {
                ScheduledFuture<?> previous = currentAttempt.get();
                if (currentAttempt.compareAndSet(previous, localFuture)) {
                    if (previous != null)
                        previous.cancel(false);
                    break;
                }
            }
            readyForNext = true;
        } catch (RejectedExecutionException e) {
            // The executor has been shutdown, fair enough, just ignore
            logger.debug("Aborting reconnection handling since the cluster is shutting down");
        }
    }

    @Override
    public void run() {
        // We shouldn't arrive here if the future is cancelled but better safe than sorry
        if (localFuture.isCancelled())
            return;

        // Don't run before ready, otherwise our cancel business might end up removing all connection attempts.
        while (!readyForNext)
            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.MILLISECONDS);

        try {
            onReconnection(tryReconnect());
            currentAttempt.compareAndSet(localFuture, null);
        } catch (ConnectionException e) {
            long nextDelay = schedule.nextDelayMs();
            if (onConnectionException(e, nextDelay))
                reschedule(nextDelay);
            else
                currentAttempt.compareAndSet(localFuture, null);
        } catch (AuthenticationException e) {
            logger.error(e.getMessage());
            long nextDelay = schedule.nextDelayMs();
            if (onAuthenticationException(e, nextDelay)) {
                reschedule(nextDelay);
            } else {
                logger.error("Retry against {} have been suspended. It won't be retried unless the node is restarted.", e.getHost());
                currentAttempt.compareAndSet(localFuture, null);
            }
        } catch (InterruptedException e) {
            // If interrupted, skip this attempt but still skip scheduling reconnections
            Thread.currentThread().interrupt();
            reschedule(schedule.nextDelayMs());
        } catch (Exception e) {
            long nextDelay = schedule.nextDelayMs();
            if (onUnknownException(e, nextDelay))
                reschedule(nextDelay);
            else
                currentAttempt.compareAndSet(localFuture, null);
        }
    }

    private void reschedule(long nextDelay) {
        readyForNext = false;
        ScheduledFuture<?> newFuture = executor.schedule(this, nextDelay, TimeUnit.MILLISECONDS);
        assert localFuture != null;
        // If it's not our future the current one, then we've been canceled
        if (!currentAttempt.compareAndSet(localFuture, newFuture)) {
            newFuture.cancel(false);
        }
        localFuture = newFuture;
        readyForNext = true;
    }
}
