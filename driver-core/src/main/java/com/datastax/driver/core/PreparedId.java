/*
 * Copyright DataStax, Inc.
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

/** Identifies a PreparedStatement. */
public class PreparedId {

  // This class is mostly here to group PreparedStatement data that are needed for
  // execution but that we don't want to expose publicly (see JAVA-195)

  final int[] routingKeyIndexes;

  final ProtocolVersion protocolVersion;

  final PreparedMetadata boundValuesMetadata;

  // can change over time, see JAVA-1196, JAVA-420
  volatile PreparedMetadata resultSetMetadata;

  PreparedId(
      PreparedMetadata boundValuesMetadata,
      PreparedMetadata resultSetMetadata,
      int[] routingKeyIndexes,
      ProtocolVersion protocolVersion) {
    assert boundValuesMetadata != null;
    assert resultSetMetadata != null;
    this.boundValuesMetadata = boundValuesMetadata;
    this.resultSetMetadata = resultSetMetadata;
    this.routingKeyIndexes = routingKeyIndexes;
    this.protocolVersion = protocolVersion;
  }

  static class PreparedMetadata {

    final MD5Digest id;
    final ColumnDefinitions variables;

    PreparedMetadata(MD5Digest id, ColumnDefinitions variables) {
      this.id = id;
      this.variables = variables;
    }
  }
}
