package com.datastax.driver.core.exceptions;

import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.Map;

import com.beust.jcommander.internal.Maps;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class NoHostAvailableExceptionTest {
    @Test(groups = "unit")
    public void should_build_default_message_when_less_than_3_errors() {
        NoHostAvailableException e = new NoHostAvailableException(buildMockErrors(3));
        assertThat(e.getMessage())
            .isEqualTo("All host(s) tried for query failed (tried: "
                    + "/127.0.0.1:9042 (com.datastax.driver.core.exceptions.NoHostAvailableExceptionTest$MockError: mock error 1), "
                    + "/127.0.0.3:9042 (com.datastax.driver.core.exceptions.NoHostAvailableExceptionTest$MockError: mock error 3), "
                    + "/127.0.0.2:9042 (com.datastax.driver.core.exceptions.NoHostAvailableExceptionTest$MockError: mock error 2)"
                    + ")"
            );
    }

    @Test(groups = "unit")
    public void should_build_default_message_when_more_than_3_errors() {
        NoHostAvailableException e = new NoHostAvailableException(buildMockErrors(4));
        assertThat(e.getMessage())
            .isEqualTo("All host(s) tried for query failed (tried: "
                    + "/127.0.0.1:9042 (com.datastax.driver.core.exceptions.NoHostAvailableExceptionTest$MockError: mock error 1), "
                    + "/127.0.0.3:9042 (com.datastax.driver.core.exceptions.NoHostAvailableExceptionTest$MockError: mock error 3), "
                    + "/127.0.0.2:9042 (com.datastax.driver.core.exceptions.NoHostAvailableExceptionTest$MockError: mock error 2), "
                    + "/127.0.0.4:9042 "
                    + "[only showing errors of first 3 hosts, use getErrors() for more details]"
                    + ")"
            );
    }

    @Test(groups = "unit")
    public void should_build_formatted_message_without_stack_traces() {
        NoHostAvailableException e = new NoHostAvailableException(buildMockErrors(3));
        assertThat(e.getCustomMessage(3, true, false))
            .isEqualTo("All host(s) tried for query failed (tried:\n"
                    + "/127.0.0.1:9042 (com.datastax.driver.core.exceptions.NoHostAvailableExceptionTest$MockError: mock error 1)\n"
                    + "/127.0.0.3:9042 (com.datastax.driver.core.exceptions.NoHostAvailableExceptionTest$MockError: mock error 3)\n"
                    + "/127.0.0.2:9042 (com.datastax.driver.core.exceptions.NoHostAvailableExceptionTest$MockError: mock error 2)\n"
                    + ")"
            );
    }

    @Test(groups = "unit")
    public void should_build_formatted_message_with_stack_traces() {
        NoHostAvailableException e = new NoHostAvailableException(buildMockErrors(3));
        assertThat(e.getCustomMessage(3, true, true))
            .isEqualTo("All host(s) tried for query failed (tried:\n"
                    + "/127.0.0.1:9042\n"
                    + "mock stack trace 1\n\n"
                    + "/127.0.0.3:9042\n"
                    + "mock stack trace 3\n\n"
                    + "/127.0.0.2:9042\n"
                    + "mock stack trace 2\n"
                    + ")"
            );
    }

    private static Map<InetSocketAddress, Throwable> buildMockErrors(int count) {
        Map<InetSocketAddress, Throwable> errors = Maps.newHashMap();
        for (int i = 1; i <= count; i++) {
            errors.put(new InetSocketAddress("127.0.0." + i, 9042), new MockError(i));
        }
        return errors;
    }

    static class MockError extends Exception {
        private final int i;

        MockError(int i) {
            super("mock error " + i);
            this.i = i;
        }

        @Override public void printStackTrace(PrintWriter writer) {
            writer.printf("mock stack trace %d", i);
        }
    }
}