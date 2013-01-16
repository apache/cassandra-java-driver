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

    private final Map<InetAddress, String> errors;

    public NoHostAvailableException(Map<InetAddress, String> errors) {
        super(makeMessage(errors));
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

    private static String makeMessage(Map<InetAddress, String> errors) {
        return String.format("All host(s) tried for query failed (tried: %s)", errors.keySet());
    }
}
