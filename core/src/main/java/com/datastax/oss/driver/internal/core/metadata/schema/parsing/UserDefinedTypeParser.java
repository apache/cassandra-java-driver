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
package com.datastax.oss.driver.internal.core.metadata.schema.parsing;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.type.DefaultUserDefinedType;
import com.datastax.oss.driver.internal.core.util.DirectedGraph;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class UserDefinedTypeParser {
  private final DataTypeParser dataTypeParser;
  private final InternalDriverContext context;

  public UserDefinedTypeParser(DataTypeParser dataTypeParser, InternalDriverContext context) {
    this.dataTypeParser = dataTypeParser;
    this.context = context;
  }

  /**
   * Contrary to other element parsers, this one processes all the types of a keyspace in one go.
   * UDTs can depend on each other, but the system table returns them in alphabetical order. In
   * order to properly build the definitions, we need to do a topological sort of the rows first, so
   * that each type is parsed after its dependencies.
   */
  public Map<CqlIdentifier, UserDefinedType> parse(
      Collection<AdminRow> typeRows, CqlIdentifier keyspaceId) {
    if (typeRows.isEmpty()) {
      return Collections.emptyMap();
    } else {
      Map<CqlIdentifier, UserDefinedType> types = new LinkedHashMap<>();
      for (AdminRow row : topologicalSort(typeRows, keyspaceId)) {
        UserDefinedType type = parseType(row, keyspaceId, types);
        types.put(type.getName(), type);
      }
      return ImmutableMap.copyOf(types);
    }
  }

  @VisibleForTesting
  Map<CqlIdentifier, UserDefinedType> parse(CqlIdentifier keyspaceId, AdminRow... typeRows) {
    return parse(Arrays.asList(typeRows), keyspaceId);
  }

  private List<AdminRow> topologicalSort(Collection<AdminRow> typeRows, CqlIdentifier keyspaceId) {
    if (typeRows.size() == 1) {
      AdminRow row = typeRows.iterator().next();
      return Collections.singletonList(row);
    } else {
      DirectedGraph<AdminRow> graph = new DirectedGraph<>(typeRows);
      for (AdminRow dependent : typeRows) {
        for (AdminRow dependency : typeRows) {
          if (dependent != dependency && dependsOn(dependent, dependency, keyspaceId)) {
            // Edges mean "is depended upon by"; we want the types with no dependencies to come
            // first in the sort.
            graph.addEdge(dependency, dependent);
          }
        }
      }
      return graph.topologicalSort();
    }
  }

  private boolean dependsOn(AdminRow dependent, AdminRow dependency, CqlIdentifier keyspaceId) {
    CqlIdentifier dependencyId = CqlIdentifier.fromInternal(dependency.getString("type_name"));
    for (String fieldTypeName : dependent.getListOfString("field_types")) {
      DataType fieldType = dataTypeParser.parse(keyspaceId, fieldTypeName, null, context);
      if (references(fieldType, dependencyId)) {
        return true;
      }
    }
    return false;
  }

  private boolean references(DataType dependent, CqlIdentifier dependency) {
    if (dependent instanceof UserDefinedType) {
      UserDefinedType userType = (UserDefinedType) dependent;
      return userType.getName().equals(dependency);
    } else if (dependent instanceof ListType) {
      ListType listType = (ListType) dependent;
      return references(listType.getElementType(), dependency);
    } else if (dependent instanceof SetType) {
      SetType setType = (SetType) dependent;
      return references(setType.getElementType(), dependency);
    } else if (dependent instanceof MapType) {
      MapType mapType = (MapType) dependent;
      return references(mapType.getKeyType(), dependency)
          || references(mapType.getValueType(), dependency);
    } else if (dependent instanceof TupleType) {
      TupleType tupleType = (TupleType) dependent;
      for (DataType componentType : tupleType.getComponentTypes()) {
        if (references(componentType, dependency)) {
          return true;
        }
      }
    }
    return false;
  }

  private UserDefinedType parseType(
      AdminRow row,
      CqlIdentifier keyspaceId,
      Map<CqlIdentifier, UserDefinedType> userDefinedTypes) {
    // Cassandra < 3.0:
    // CREATE TABLE system.schema_usertypes (
    //     keyspace_name text,
    //     type_name text,
    //     field_names list<text>,
    //     field_types list<text>,
    //     PRIMARY KEY (keyspace_name, type_name)
    // ) WITH CLUSTERING ORDER BY (type_name ASC)
    //
    // Cassandra >= 3.0:
    // CREATE TABLE system_schema.types (
    //     keyspace_name text,
    //     type_name text,
    //     field_names frozen<list<text>>,
    //     field_types frozen<list<text>>,
    //     PRIMARY KEY (keyspace_name, type_name)
    // ) WITH CLUSTERING ORDER BY (type_name ASC)
    CqlIdentifier name = CqlIdentifier.fromInternal(row.getString("type_name"));
    List<CqlIdentifier> fieldNames =
        ImmutableList.copyOf(
            Lists.transform(row.getListOfString("field_names"), CqlIdentifier::fromInternal));
    List<DataType> fieldTypes =
        dataTypeParser.parse(
            keyspaceId, row.getListOfString("field_types"), userDefinedTypes, context);

    return new DefaultUserDefinedType(keyspaceId, name, false, fieldNames, fieldTypes, context);
  }
}
