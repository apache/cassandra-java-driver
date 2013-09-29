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
package com.datastax.driver.core.exceptions;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * Exception thrown when a query cannot be performed because no host are
 * available.
 *
 * This exception is thrown if
 * <ul>
 *   <li>either there is no host live in the cluster at the moment of the query</li>
 *   <li>all host that have been tried have failed due to a connection problem</li>
 * </ul>
 *
 * For debugging purpose, the list of hosts that have been tried along with the
 * failure cause can be retrieved using the {@link #errors} method.
 */
public class NoHostAvailableException extends DriverException {

    private static final long serialVersionUID = 0;

    private final Map<InetAddress, String> errors;

    public NoHostAvailableException(Map<InetAddress, String> errors) {
        super(makeMessage(errors));
        this.errors = errors;
    }

    private NoHostAvailableException(String message, Throwable cause, Map<InetAddress, String> errors) {
        super(message, cause);
        this.errors = errors;
    }

    /**
     * Return the hosts tried along with descriptions of the error encountered
     * while trying them.
     *
     * @return a map containing for each tried host a description of the error
     * triggered when trying it.
     */
    public Map<InetAddress, String> getErrors() {
        return new HashMap<InetAddress, String>(errors);
    }

    @Override
    public DriverException copy() {
        return new NoHostAvailableException(getMessage(), this, errors);
    }

    private static String makeMessage(Map<InetAddress, String> errors) {
        // For small cluster, ship the whole error detail in the error message.
        // This is helpful when debugging on a localhost/test cluster in particular.
        if (errors.size() == 0)
            return "All host(s) tried for query failed (no host was tried)";

        if (errors.size() <= 3) {
            StringBuilder sb = new StringBuilder();
            sb.append("All host(s) tried for query failed (tried: ");
            int n = 0;
            for (Map.Entry<InetAddress, String> entry : errors.entrySet())
            {
                if (n++ > 0) sb.append(", ");
                sb.append(entry.getKey()).append(" (").append(entry.getValue()).append(")");
            }
            return sb.append(")").toString();
        }
        return String.format("All host(s) tried for query failed (tried: %s - use getErrors() for details)", errors.keySet());
    }
}
