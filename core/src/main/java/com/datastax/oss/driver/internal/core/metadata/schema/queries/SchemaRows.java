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
package com.datastax.oss.driver.internal.core.metadata.schema.queries;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.DataTypeParser;
import com.datastax.oss.driver.shaded.guava.common.collect.Multimap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * The system rows returned by the queries for a schema refresh, categorized by keyspace/table where
 * relevant.
 *
 * <p>Implementations must be thread-safe.
 */
public interface SchemaRows {

  List<AdminRow> keyspaces();

  Multimap<CqlIdentifier, AdminRow> tables();

  Multimap<CqlIdentifier, AdminRow> views();

  Multimap<CqlIdentifier, AdminRow> types();

  Multimap<CqlIdentifier, AdminRow> functions();

  Multimap<CqlIdentifier, AdminRow> aggregates();

  Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> columns();

  Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> indexes();

  DataTypeParser dataTypeParser();

  /**
   * The future to complete when the schema refresh is complete (here just to be propagated further
   * down the chain).
   */
  CompletableFuture<Metadata> refreshFuture();
}
