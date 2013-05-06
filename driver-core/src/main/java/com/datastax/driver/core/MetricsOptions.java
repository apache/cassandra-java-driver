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
package com.datastax.driver.core;

/**
 * {@link Metrics} options.
 */
public class MetricsOptions {

    private final boolean jmxEnabled;

    /**
     * Creates a new {@code MetricsOptions} object with default values.
     */
    public MetricsOptions()
    {
        this(true);
    }

    /**
     * Creates a new {@code MetricsOptions} object.
     *
     * @param jmxEnabled whether to enable JMX reporting or not.
     */
    public MetricsOptions(boolean jmxEnabled)
    {
        this.jmxEnabled = jmxEnabled;
    }

    /**
     * Returns whether JMX reporting is enabled (the default).
     *
     * @return whether JMX reporting is enabled.
     */
    public boolean isJMXReportingEnabled()
    {
        return jmxEnabled;
    }
}
