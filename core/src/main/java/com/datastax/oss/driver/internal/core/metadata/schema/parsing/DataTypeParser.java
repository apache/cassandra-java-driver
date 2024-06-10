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
package com.datastax.oss.driver.internal.core.metadata.schema.parsing;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.schema.ShallowUserDefinedType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Parses data types from their string representation in schema tables. */
public interface DataTypeParser {

  /**
   * @param userTypes the UDTs in the current keyspace, if we know them already. This is used to
   *     resolve subtypes if the type to parse is complex (such as {@code list<foo>}). The only
   *     situation where we don't have them is when we refresh all the UDTs of a keyspace; in that
   *     case, the filed will be {@code null} and any UDT encountered by this method will always be
   *     re-created from scratch: for Cassandra &lt; 2.2, this means parsing the whole definition;
   *     for &gt; 3.0, this means materializing it as a {@link ShallowUserDefinedType} that will be
   *     resolved in a second pass.
   */
  DataType parse(
      CqlIdentifier keyspaceId,
      String toParse,
      Map<CqlIdentifier, UserDefinedType> userTypes,
      InternalDriverContext context);

  default List<DataType> parse(
      CqlIdentifier keyspaceId,
      List<String> typeStrings,
      Map<CqlIdentifier, UserDefinedType> userTypes,
      InternalDriverContext context) {
    if (typeStrings.isEmpty()) {
      return Collections.emptyList();
    } else {
      ImmutableList.Builder<DataType> builder = ImmutableList.builder();
      for (String typeString : typeStrings) {
        builder.add(parse(keyspaceId, typeString, userTypes, context));
      }
      return builder.build();
    }
  }
}
