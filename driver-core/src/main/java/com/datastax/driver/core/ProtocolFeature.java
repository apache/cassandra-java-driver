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
 * A listing of features that may or not apply to a given {@link ProtocolVersion}.
 */
enum ProtocolFeature {

    /**
     * The capability of updating a prepared statement if the result's metadata changes at runtime (for example, if the
     * query is a {@code SELECT *} and the table is altered).
     */
    PREPARED_METADATA_CHANGES,
    //
    ;

    /**
     * Determines whether or not the input version supports ths feature.
     *
     * @param version the version to test against.
     * @return true if supported, false otherwise.
     */
    boolean isSupportedBy(ProtocolVersion version) {
        switch (this) {
            case PREPARED_METADATA_CHANGES:
                return version == ProtocolVersion.V5;
            default:
                return false;
        }
    }

}
