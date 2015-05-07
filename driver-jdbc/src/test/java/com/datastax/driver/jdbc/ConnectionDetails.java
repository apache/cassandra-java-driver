package com.datastax.driver.jdbc;

import java.io.InputStream;
import java.util.Properties;

/**
 * Load the connection details from the properties file.
 */
public final class ConnectionDetails {

    private static final ConnectionDetails INSTANCE = new ConnectionDetails();

    private final String host;

    private final int port;

    private ConnectionDetails() {
        Properties p = new Properties();
        InputStream stream = getClass().getResourceAsStream(getClass().getSimpleName() + ".properties");
        try {
            p.load(stream);
        } catch (Exception e) {
            // ignore, we'll use the defaults
        } finally {
            try {
                stream.close();
            } catch (Exception e) {
                // ignore
            }
        }
        
        host = p.getProperty("host", "localhost");
        int port;
        try {
            port = Integer.parseInt(p.getProperty("port", "9042"));
        } catch (NumberFormatException e) {
            port = 9042;
        }
        this.port = port;
    }

    public static String getHost() {
        return INSTANCE.host;
    }

    public static int getPort() {
        return INSTANCE.port;
    }
}
