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
package com.datastax.oss.driver.internal.querybuilder.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.schema.CreateType;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTypeStart;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultCreateType implements CreateTypeStart, CreateType {

  private final CqlIdentifier keyspace;
  private final CqlIdentifier typeName;
  private final boolean ifNotExists;
  private final ImmutableMap<CqlIdentifier, DataType> fieldsInOrder;

  public DefaultCreateType(@NonNull CqlIdentifier typeName) {
    this(null, typeName);
  }

  public DefaultCreateType(@Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier typeName) {
    this(keyspace, typeName, false, ImmutableMap.of());
  }

  public DefaultCreateType(
      @Nullable CqlIdentifier keyspace,
      @NonNull CqlIdentifier typeName,
      boolean ifNotExists,
      @NonNull ImmutableMap<CqlIdentifier, DataType> fieldsInOrder) {
    this.keyspace = keyspace;
    this.typeName = typeName;
    this.ifNotExists = ifNotExists;
    this.fieldsInOrder = fieldsInOrder;
  }

  @NonNull
  @Override
  public CreateType withField(@NonNull CqlIdentifier fieldName, @NonNull DataType dataType) {
    return new DefaultCreateType(
        keyspace,
        typeName,
        ifNotExists,
        ImmutableCollections.append(fieldsInOrder, fieldName, dataType));
  }

  @NonNull
  @Override
  public CreateTypeStart ifNotExists() {
    return new DefaultCreateType(keyspace, typeName, true, fieldsInOrder);
  }

  @NonNull
  @Override
  public String asCql() {
    StringBuilder builder = new StringBuilder();

    builder.append("CREATE TYPE ");
    if (ifNotExists) {
      builder.append("IF NOT EXISTS ");
    }

    CqlHelper.qualify(keyspace, typeName, builder);

    if (fieldsInOrder.isEmpty()) {
      // no fields provided yet.
      return builder.toString();
    }

    builder.append(" (");

    boolean first = true;
    for (Map.Entry<CqlIdentifier, DataType> field : fieldsInOrder.entrySet()) {
      if (first) {
        first = false;
      } else {
        builder.append(',');
      }
      builder
          .append(field.getKey().asCql(true))
          .append(' ')
          .append(field.getValue().asCql(true, true));
    }
    builder.append(')');
    return builder.toString();
  }

  @Override
  public String toString() {
    return asCql();
  }

  @Nullable
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @NonNull
  public CqlIdentifier getType() {
    return typeName;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  @NonNull
  public ImmutableMap<CqlIdentifier, DataType> getFieldsInOrder() {
    return fieldsInOrder;
  }
}
