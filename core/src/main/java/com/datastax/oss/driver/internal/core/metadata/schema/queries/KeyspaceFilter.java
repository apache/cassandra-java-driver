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
package com.datastax.oss.driver.internal.core.metadata.schema.queries;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;

/**
 * Filters keyspaces during schema metadata queries.
 *
 * <p>Depending on the circumstances, we do it either on the server side with a WHERE IN clause that
 * will be appended to every query, or on the client side with a predicate that will be applied to
 * every fetched row.
 */
public interface KeyspaceFilter {

  static KeyspaceFilter newInstance(@NonNull String logPrefix, @NonNull List<String> specs) {
    if (specs.isEmpty()) {
      return INCLUDE_ALL;
    } else {
      return new RuleBasedKeyspaceFilter(logPrefix, specs);
    }
  }

  /** The WHERE IN clause, or an empty string if there is no server-side filtering. */
  @NonNull
  String getWhereClause();

  /** The predicate that will be invoked for client-side filtering. */
  boolean includes(@NonNull String keyspace);

  KeyspaceFilter INCLUDE_ALL =
      new KeyspaceFilter() {
        @NonNull
        @Override
        public String getWhereClause() {
          return "";
        }

        @Override
        public boolean includes(@NonNull String keyspace) {
          return true;
        }
      };
}
