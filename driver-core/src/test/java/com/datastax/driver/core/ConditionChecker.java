/*
 *      Copyright (C) 2012-2015 DataStax Inc.
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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Fail.fail;

public class ConditionChecker {

    private static final Logger logger = LoggerFactory.getLogger(ConditionChecker.class);

    private static final int DEFAULT_PERIOD_MILLIS = 500;

    public static <T> void awaitUntil(T input, Predicate<? super T> predicate, long timeoutMillis) {
        new ConditionChecker(input, predicate).await(timeoutMillis);
    }

    public static void awaitUntil(final Callable<Boolean> condition, long timeoutMillis) {
        new ConditionChecker(condition).await(timeoutMillis);
    }

    public static <T> void awaitWhile(T input, Predicate<? super T> predicate, long timeoutMillis) {
        new ConditionChecker(input, Predicates.not(predicate)).await(timeoutMillis);
    }

    public static void awaitWhile(final Callable<Boolean> condition, long timeoutMillis) {
        new ConditionChecker(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return !condition.call();
            }
        }).await(timeoutMillis);
    }

    private final Object input;

    private final Predicate<Object> predicate;

    private final Lock lock;

    private final Condition condition;

    private final Timer timer;

    /**
     * When using this constructor, one must override {@link #evalCondition()}.
     */
    public ConditionChecker() {
        this(null, null);
        checkState(!this.getClass().equals(ConditionChecker.class), "This constructor requires subclassing ConditionChecker");
    }

    public ConditionChecker(Callable<Boolean> condition) {
        this(condition, DEFAULT_PERIOD_MILLIS);
    }

    public ConditionChecker(final Callable<Boolean> condition, int periodMillis) {
        this(null, new Predicate<Void>() {
            @Override
            public boolean apply(Void input) {
                try {
                    return condition.call();
                } catch (Exception e) {
                    logger.error("Evaluation of condition threw exception", e);
                    return false;
                }
            }
        }, periodMillis);
    }

    public <T> ConditionChecker(T input, Predicate<? super T> predicate) {
        this(input, predicate, DEFAULT_PERIOD_MILLIS);
    }

    @SuppressWarnings("unchecked")
    public <T> ConditionChecker(T input, Predicate<? super T> predicate, int periodMillis) {
        this.input = input;
        this.predicate = (Predicate<Object>) predicate;
        lock = new ReentrantLock();
        condition = lock.newCondition();
        timer = new Timer("condition-checker", true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                checkCondition();
            }
        }, 0, periodMillis);
    }

    /**
     * Waits until the predicate becomes true,
     * or a timeout occurs, whichever happens first.
     *
     * @param timeoutMillis timeout in milliseconds
     */
    public void await(long timeoutMillis) {
        boolean interrupted = false;
        long nanos = MILLISECONDS.toNanos(timeoutMillis);
        lock.lock();
        try {
            while (!evalCondition()) {
                if (nanos <= 0L)
                    fail(String.format("Timeout after %s milliseconds while waiting for condition", timeoutMillis));
                try {
                    nanos = condition.awaitNanos(nanos);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            timer.cancel();
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    private void checkCondition() {
        lock.lock();
        try {
            if (evalCondition()) {
                condition.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    protected boolean evalCondition() {
        return predicate.apply(input);
    }

}
