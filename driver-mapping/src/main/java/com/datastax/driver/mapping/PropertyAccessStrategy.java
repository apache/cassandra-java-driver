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
package com.datastax.driver.mapping;

/**
 * A strategy to determine how mapped properties are discovered,
 * and how to access them.
 */
public enum PropertyAccessStrategy {

    /**
     * Use getters and setters exclusively. These must be available for all mapped properties.
     */
    GETTERS_AND_SETTERS,

    /**
     * Use field access exclusively. Fields do not need to be declared public,
     * the driver will attempt to make them accessible via reflection if required.
     */
    FIELDS,

    /**
     * Use getters and setters preferably, and if these are not available,
     * use field access. Fields do not need to be declared public,
     * the driver will attempt to make them accessible via reflection if required.
     * This is the default access strategy.
     */
    BOTH;

    /**
     * Returns {@code true} if field scan is allowed, {@code false} otherwise.
     *
     * @return {@code true} if field access is allowed, {@code false} otherwise.
     */
    public boolean isFieldScanAllowed() {
        return this == FIELDS || this == BOTH;
    }

    /**
     * Returns {@code true} if getter and setter scan is allowed, {@code false} otherwise.
     *
     * @return {@code true} if getter and setter access is allowed, {@code false} otherwise.
     */
    public boolean isGetterSetterScanAllowed() {
        return this == GETTERS_AND_SETTERS || this == BOTH;
    }

}
