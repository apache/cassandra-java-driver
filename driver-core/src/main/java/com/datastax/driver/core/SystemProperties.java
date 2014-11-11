package com.datastax.driver.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allows overriding internal settings via system properties.
 * <p>
 * Warning: this is meant for integration tests only, NOT FOR PRODUCTION USE.
 */
class SystemProperties {
    private static final Logger logger = LoggerFactory.getLogger(SystemProperties.class);

    static int getInt(String key, int defaultValue) {
        String stringValue = System.getProperty(key);
        if (stringValue == null) {
            logger.debug("{} is undefined, using default value {}", key, defaultValue);
            return defaultValue;
        }
        try {
            int value = Integer.parseInt(stringValue);
            logger.warn("{} is defined, using value {}", key, value);
            return value;
        } catch (NumberFormatException e) {
            logger.warn("{} is defined but could not parse value {}, using default value {}", key, stringValue, defaultValue);
            return defaultValue;
        }
    }

    static boolean getBoolean(String key, boolean defaultValue) {
        String stringValue = System.getProperty(key);
        if (stringValue == null) {
            logger.debug("{} is undefined, using default value {}", key, defaultValue);
            return defaultValue;
        }
        try {
            boolean value = Boolean.parseBoolean(stringValue);
            logger.warn("{} is defined, using value {}", key, value);
            return value;
        } catch (NumberFormatException e) {
            logger.warn("{} is defined but could not parse value {}, using default value {}", key, stringValue, defaultValue);
            return defaultValue;
        }
    }
}
