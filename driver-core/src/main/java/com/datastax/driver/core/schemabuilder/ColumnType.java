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
package com.datastax.driver.core.schemabuilder;

import com.datastax.driver.core.DataType;

/**
 * Wrapper around UDT and non-UDT types.
 * <p>
 * The reason for this interface is that the core API doesn't let us build {@link com.datastax.driver.core.DataType}s representing UDTs, we have to obtain
 * them from the cluster metadata. Since we want to use SchemaBuilder without a Cluster instance, UDT types will be provided via
 * {@link UDTType} instances.
 */
interface ColumnType {
    String asCQLString();

    class NativeColumnType implements ColumnType {
        private final String asCQLString;

        NativeColumnType(DataType nativeType) {
            asCQLString = nativeType.toString();
        }

        @Override public String asCQLString() {
            return asCQLString;
        }
    }
}
