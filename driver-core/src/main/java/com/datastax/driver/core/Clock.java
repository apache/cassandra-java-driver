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

/**
 * This interface allows us not to have a direct call to {@code System.currentTimeMillis()} for testing purposes
 */
interface Clock {
    /**
     * Returns the current time in milliseconds
     *
     * @return the difference, measured in milliseconds, between the current time and midnight, January 1, 1970 UTC.
     * @see System#currentTimeMillis()
     */
    long currentTime();
}

/**
 * Default implementation of a clock that delegate its calls to the system clock.
 */
class SystemClock implements Clock {
    @Override
    public long currentTime() {
        return System.currentTimeMillis();
    }
}