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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.DataTypeParser;
import com.datastax.oss.driver.shaded.guava.common.collect.Multimap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The system rows returned by the queries for a schema refresh, categorized by keyspace/table where
 * relevant.
 *
 * <p>Implementations must be thread-safe.
 */
public interface SchemaRows {

  /** The node that was used to retrieve the schema information. */
  @NonNull
  Node getNode();

  List<AdminRow> keyspaces();

  List<AdminRow> virtualKeyspaces();

  Multimap<CqlIdentifier, AdminRow> tables();

  Multimap<CqlIdentifier, AdminRow> virtualTables();

  Multimap<CqlIdentifier, AdminRow> views();

  Multimap<CqlIdentifier, AdminRow> types();

  Multimap<CqlIdentifier, AdminRow> functions();

  Multimap<CqlIdentifier, AdminRow> aggregates();

  Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> columns();

  Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> virtualColumns();

  Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> indexes();

  DataTypeParser dataTypeParser();

  default Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> vertices() {
    return new LinkedHashMap<>();
  }

  default Map<CqlIdentifier, Multimap<CqlIdentifier, AdminRow>> edges() {
    return new LinkedHashMap<>();
  }
}
