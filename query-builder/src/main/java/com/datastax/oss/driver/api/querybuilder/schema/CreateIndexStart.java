/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.api.querybuilder.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;

public interface CreateIndexStart {

  /**
   * Adds IF NOT EXISTS to the create index specification. This indicates that the index should not
   * be created if it already exists.
   */
  @NonNull
  CreateIndexStart ifNotExists();

  /**
   * Adds CUSTOM specification to the index for the given class name. The class name will added to
   * the end of the CREATE INDEX specification with <code>USING 'classname'</code>.
   */
  @NonNull
  CreateIndexStart custom(@NonNull String className);

  /**
   * Declares that the index is a "SSTable Attached Secondary Index" (SASI) type index. This is a
   * custom index with the class <code>org.apache.cassandra.index.SASIIndex</code>.
   *
   * @see CreateIndex#withSASIOptions(Map)
   */
  @NonNull
  default CreateIndexStart usingSASI() {
    return custom("org.apache.cassandra.index.sasi.SASIIndex");
  }

  /** Indicates which table this index is on. */
  @NonNull
  CreateIndexOnTable onTable(@Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier table);

  /**
   * Indicates which table this index is on. This assumes the keyspace name is already qualified for
   * the Session or Statement.
   */
  @NonNull
  default CreateIndexOnTable onTable(@NonNull CqlIdentifier table) {
    return onTable(null, table);
  }

  /**
   * Shortcut for {@link #onTable(CqlIdentifier,CqlIdentifier)
   * onTable(CqlIdentifier.fromCql(keyspace),CqlIdentifier.fromCql(table))}.
   */
  @NonNull
  default CreateIndexOnTable onTable(@Nullable String keyspace, @NonNull String table) {
    return onTable(
        keyspace == null ? null : CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(table));
  }

  /** Shortcut for {@link #onTable(CqlIdentifier) onTable(CqlIdentifier.fromCql(table))}. */
  @NonNull
  default CreateIndexOnTable onTable(@NonNull String table) {
    return onTable(CqlIdentifier.fromCql(table));
  }
}
