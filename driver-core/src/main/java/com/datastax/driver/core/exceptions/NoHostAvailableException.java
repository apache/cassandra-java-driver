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
package com.datastax.driver.core.exceptions;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * Exception thrown when a query cannot be performed because no host is
 * available.
 * <p/>
 * This exception is thrown if either:
 * <ul>
 * <li>there is no host live in the cluster at the moment of the query;</li>
 * <li>all hosts that have been tried have failed.</li>
 * </ul>
 * <p/>
 * For debugging purposes, the list of hosts that have been tried along with the
 * failure cause can be retrieved using the {@link #getErrors()} method.
 */
public class NoHostAvailableException extends DriverException {

    private static final long serialVersionUID = 0;

    private static final int MAX_ERRORS_IN_DEFAULT_MESSAGE = 3;

    private final Map<InetSocketAddress, Throwable> errors;

    public NoHostAvailableException(Map<InetSocketAddress, Throwable> errors) {
        super(makeMessage(errors, MAX_ERRORS_IN_DEFAULT_MESSAGE, false, false));
        this.errors = errors;
    }

    private NoHostAvailableException(String message, Throwable cause, Map<InetSocketAddress, Throwable> errors) {
        super(message, cause);
        this.errors = errors;
    }

    /**
     * Return the hosts tried along with the error encountered while trying
     * them.
     *
     * @return a map containing for each tried host the error triggered when
     * trying it.
     */
    public Map<InetSocketAddress, Throwable> getErrors() {
        return new HashMap<InetSocketAddress, Throwable>(errors);
    }

    /**
     * Builds a custom message for this exception.
     *
     * @param maxErrors          the maximum number of errors displayed (useful to limit the size of the message for big clusters). Beyond this limit,
     *                           host names are still displayed, but not the associated errors. Set to {@code Integer.MAX_VALUE} to display all hosts.
     * @param formatted          whether to format the output (line break between each host).
     * @param includeStackTraces whether to include the full stacktrace of each host error. Note that this automatically implies
     *                           {@code formatted}.
     * @return the message.
     */
    public String getCustomMessage(int maxErrors, boolean formatted, boolean includeStackTraces) {
        if (includeStackTraces)
            formatted = true;
        return makeMessage(errors, maxErrors, formatted, includeStackTraces);
    }

    @Override
    public NoHostAvailableException copy() {
        return new NoHostAvailableException(getMessage(), this, errors);
    }

    private static String makeMessage(Map<InetSocketAddress, Throwable> errors, int maxErrorsInMessage, boolean formatted, boolean includeStackTraces) {
        if (errors.size() == 0)
            return "All host(s) tried for query failed (no host was tried)";

        StringWriter stringWriter = new StringWriter();
        PrintWriter out = new PrintWriter(stringWriter);

        out.print("All host(s) tried for query failed (tried:");
        out.print(formatted ? "\n" : " ");

        int n = 0;
        boolean truncated = false;
        for (Map.Entry<InetSocketAddress, Throwable> entry : errors.entrySet()) {
            if (n > 0) out.print(formatted ? "\n" : ", ");
            out.print(entry.getKey());
            if (n < maxErrorsInMessage) {
                if (includeStackTraces) {
                    out.print("\n");
                    entry.getValue().printStackTrace(out);
                    out.print("\n");
                } else {
                    out.printf(" (%s)", entry.getValue());
                }
            } else {
                truncated = true;
            }
            n += 1;
        }
        if (truncated) {
            out.print(formatted ? "\n" : " ");
            out.printf("[only showing errors of first %d hosts, use getErrors() for more details]", maxErrorsInMessage);
        }
        if (formatted && !includeStackTraces)
            out.print("\n");
        out.print(")");
        out.close();
        return stringWriter.toString();
    }
}
