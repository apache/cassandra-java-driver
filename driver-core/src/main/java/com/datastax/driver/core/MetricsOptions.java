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

/**
 * {@link Metrics} options.
 */
public class MetricsOptions {

    private final boolean metricsEnabled;
    private final boolean jmxEnabled;

    /**
     * Creates a new {@code MetricsOptions} object with default values (metrics enabled, JMX reporting enabled).
     */
    public MetricsOptions() {
        this(true, true);
    }

    /**
     * Creates a new {@code MetricsOptions} object.
     *
     * @param jmxEnabled whether to enable JMX reporting or not.
     */
    public MetricsOptions(boolean enabled, boolean jmxEnabled) {
        this.metricsEnabled = enabled;
        this.jmxEnabled = jmxEnabled;
    }

    /**
     * Returns whether metrics are enabled.
     *
     * @return whether metrics are enabled.
     */
    public boolean isEnabled() {
        return metricsEnabled;
    }

    /**
     * Returns whether JMX reporting is enabled.
     *
     * @return whether JMX reporting is enabled.
     */
    public boolean isJMXReportingEnabled() {
        return jmxEnabled;
    }
}
