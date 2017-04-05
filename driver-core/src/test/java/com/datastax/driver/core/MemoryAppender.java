/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.apache.log4j.spi.LoggingEvent;

import java.io.StringWriter;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Simple Log4J appender that captures logs to memory in order to inspect them in unit tests.
 * <p/>
 * There is no purging mechanism, so make sure it doesn't stay enabled for too long (this is best
 * done with an {@code @After} method that removes it).
 */
public class MemoryAppender extends WriterAppender {

    private final Lock appendLock = new ReentrantLock();

    private final Condition append = appendLock.newCondition();

    public final StringWriter writer = new StringWriter();

    private int nextLogIdx = 0;

    public MemoryAppender() {
        setWriter(writer);
        setLayout(new PatternLayout("%m%n"));
    }

    @Override
    protected void subAppend(LoggingEvent event) {
        appendLock.lock();
        try {
            super.subAppend(event);
            append.signal();
        } finally {
            appendLock.unlock();
        }
    }

    public String get() {
        return writer.toString();
    }

    /**
     * Wait until at least one log event is appended to the current appender,
     * or a timeout occurs, whichever happens first,
     * then return the appender contents.
     * Not thread safe.
     * Useful when asynchronous code needs to wait until
     * the appender is actually invoked at least once.
     *
     * @param timeoutMillis timeout in milliseconds
     * @return The appender contents. Not thread safe.
     */
    public String waitAndGet(long timeoutMillis) throws InterruptedException {
        long nanos = MILLISECONDS.toNanos(timeoutMillis);
        appendLock.lock();
        try {
            while (get().isEmpty()) {
                if (nanos <= 0L) break; // timeout
                nanos = append.awaitNanos(nanos);
            }
            return get();
        } finally {
            appendLock.unlock();
        }
    }

    /**
     * @return The next set of logs after getNext was last called.  Not thread safe.
     */
    public String getNext() {
        String next = get().substring(nextLogIdx);
        nextLogIdx += next.length();
        return next;
    }
}
