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
 * Identifies a PreparedStatement.
 */
public class PreparedId {
    // This class is mostly here to group PreparedStatement data that are need for
    // execution but that we don't want to expose publicly (see JAVA-195)
    final MD5Digest id;

    final ColumnDefinitions metadata;
    final ColumnDefinitions resultSetMetadata;

    final int[] routingKeyIndexes;
    final ProtocolVersion protocolVersion;

    PreparedId(MD5Digest id, ColumnDefinitions metadata, ColumnDefinitions resultSetMetadata, int[] routingKeyIndexes, ProtocolVersion protocolVersion) {
        this.id = id;
        this.metadata = metadata;
        this.resultSetMetadata = resultSetMetadata;
        this.routingKeyIndexes = routingKeyIndexes;
        this.protocolVersion = protocolVersion;
    }
}
