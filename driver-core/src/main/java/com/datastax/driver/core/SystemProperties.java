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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allows overriding internal settings via system properties.
 * <p/>
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
