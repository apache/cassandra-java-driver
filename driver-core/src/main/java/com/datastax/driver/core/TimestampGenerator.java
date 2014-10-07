/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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
 * Generates client-side, microsecond-precision query timestamps.
 * <p>
 * Implementations of this class should guarantee that, for a given thread, successive invocations
 * of {@link #next()} will yield strictly incrementing timestamps.
 */
public interface TimestampGenerator {
    /**
     * Returns the next timestamp.
     *
     * @return the next timestamp (in microseconds). If it equals {@link Long#MIN_VALUE}, it won't be
     * sent by the driver, letting Cassandra generate a server-side timestamp.
     */
    long next();
}
