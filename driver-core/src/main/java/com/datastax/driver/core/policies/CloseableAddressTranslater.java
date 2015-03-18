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
package com.datastax.driver.core.policies;

import com.datastax.driver.core.Cluster;

/**
 * Extends {@link AddressTranslater} for implementations that need to free some resources
 * at {@link Cluster} shutdown.
 * <p>
 * Note: the only reason {@link #close()} was not added directly to {@code AddressTranslater}
 * is backward-compatibility.
 */
public interface CloseableAddressTranslater extends AddressTranslater {
    /**
     * Called at {@link Cluster} shutdown.
     */
    void close();
}
