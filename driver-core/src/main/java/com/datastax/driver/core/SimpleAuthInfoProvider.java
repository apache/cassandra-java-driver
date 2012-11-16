package com.datastax.driver.core;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A simple {@code AuthInfoProvider} implementation.
 * <p>
 * This provider allows to programmatically define authentication
 * information that will then apply to all hosts.
 * <p>
 * Note that it is <b>not</b> safe to add new info to this provider once a
 * Cluster instance has been created using this provider.
 */
public class SimpleAuthInfoProvider implements AuthInfoProvider {

    private final Map<String, String> credentials = new HashMap<String, String>();

    /**
     * Creates a new, empty, simple authentication info provider.
     */
    public SimpleAuthInfoProvider() {}

    /**
     * Creates a new simple authentication info provider with the
     * informations contained in {@code properties}.
     *
     * @param properties a map of authentication information to use.
     */
    public SimpleAuthInfoProvider(Map<String, String> properties) {
        this();
        addAll(properties);
    }

    public Map<String, String> getAuthInfos(InetAddress host) {
        return credentials;
    }

    /**
     * Adds a new property to the authentication info returned by this
     * provider.
     *
     * @param property the name of the property to add.
     * @param value the value to add for {@code property}.
     * @return {@code this} object.
     */
    public SimpleAuthInfoProvider add(String property, String value) {
        credentials.put(property, value);
        return this;
    }

    /**
     * Adds all the key-value pair provided as new authentication
     * information returned by this provider.
     *
     * @param properties a map of authentication information to add.
     * @return {@code this} object.
     */
    public SimpleAuthInfoProvider addAll(Map<String, String> properties) {
        credentials.putAll(properties);
        return this;
    }
}
