package com.datastax.driver.core;

import java.io.StringWriter;

import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;

/**
 * Simple Log4J appender that captures logs to memory in order to inspect them in unit tests.
 * <p>
 * There is no purging mechanism, so make sure it doesn't stay enabled for too long (this is best
 * done with an {@code @After} method that removes it).
 */
public class MemoryAppender extends WriterAppender {
    public final StringWriter writer = new StringWriter();

    public MemoryAppender() {
        setWriter(writer);
        setLayout(new PatternLayout("%m%n"));
    }

    public String get() {
        return writer.toString();
    }
}
