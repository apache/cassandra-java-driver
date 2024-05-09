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
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTypeRenameField;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTypeRenameFieldEnd;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTypeStart;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultAlterType
    implements AlterTypeStart, AlterTypeRenameField, AlterTypeRenameFieldEnd, BuildableQuery {

  private final CqlIdentifier keyspace;
  private final CqlIdentifier typeName;

  private final CqlIdentifier fieldToAdd;
  private final DataType fieldToAddType;

  private final ImmutableMap<CqlIdentifier, CqlIdentifier> fieldsToRename;

  private final CqlIdentifier fieldToAlter;
  private final DataType fieldToAlterType;

  public DefaultAlterType(@NonNull CqlIdentifier typeName) {
    this(null, typeName);
  }

  public DefaultAlterType(@Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier typeName) {
    this(keyspace, typeName, null, null, ImmutableMap.of(), null, null);
  }

  public DefaultAlterType(
      @Nullable CqlIdentifier keyspace,
      @NonNull CqlIdentifier typeName,
      @Nullable CqlIdentifier fieldToAdd,
      @Nullable DataType fieldToAddType,
      @NonNull ImmutableMap<CqlIdentifier, CqlIdentifier> fieldsToRename,
      @Nullable CqlIdentifier fieldToAlter,
      @Nullable DataType fieldToAlterType) {
    this.keyspace = keyspace;
    this.typeName = typeName;
    this.fieldToAdd = fieldToAdd;
    this.fieldToAddType = fieldToAddType;
    this.fieldsToRename = fieldsToRename;
    this.fieldToAlter = fieldToAlter;
    this.fieldToAlterType = fieldToAlterType;
  }

  @NonNull
  @Override
  public BuildableQuery alterField(@NonNull CqlIdentifier fieldName, @NonNull DataType dataType) {
    return new DefaultAlterType(
        keyspace, typeName, fieldToAdd, fieldToAddType, fieldsToRename, fieldName, dataType);
  }

  @NonNull
  @Override
  public BuildableQuery addField(@NonNull CqlIdentifier fieldName, @NonNull DataType dataType) {
    return new DefaultAlterType(
        keyspace, typeName, fieldName, dataType, fieldsToRename, fieldToAlter, fieldToAlterType);
  }

  @NonNull
  @Override
  public AlterTypeRenameFieldEnd renameField(
      @NonNull CqlIdentifier from, @NonNull CqlIdentifier to) {
    return new DefaultAlterType(
        keyspace,
        typeName,
        fieldToAdd,
        fieldToAddType,
        ImmutableCollections.append(fieldsToRename, from, to),
        fieldToAlter,
        fieldToAlterType);
  }

  @NonNull
  @Override
  public String asCql() {
    StringBuilder builder = new StringBuilder("ALTER TYPE ");

    CqlHelper.qualify(keyspace, typeName, builder);

    if (fieldToAlter != null) {
      return builder
          .append(" ALTER ")
          .append(fieldToAlter.asCql(true))
          .append(" TYPE ")
          .append(fieldToAlterType.asCql(true, true))
          .toString();
    } else if (fieldToAdd != null) {
      return builder
          .append(" ADD ")
          .append(fieldToAdd.asCql(true))
          .append(" ")
          .append(fieldToAddType.asCql(true, true))
          .toString();
    } else if (!fieldsToRename.isEmpty()) {
      builder.append(" RENAME ");
      boolean first = true;
      for (Map.Entry<CqlIdentifier, CqlIdentifier> entry : fieldsToRename.entrySet()) {
        if (first) {
          first = false;
        } else {
          builder.append(" AND ");
        }
        builder
            .append(entry.getKey().asCql(true))
            .append(" TO ")
            .append(entry.getValue().asCql(true));
      }
      return builder.toString();
    }

    // While this is incomplete, we should return partially built query at this point for toString
    // purposes.
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

  @Nullable
  public CqlIdentifier getFieldToAdd() {
    return fieldToAdd;
  }

  @Nullable
  public DataType getFieldToAddType() {
    return fieldToAddType;
  }

  @NonNull
  public ImmutableMap<CqlIdentifier, CqlIdentifier> getFieldsToRename() {
    return fieldsToRename;
  }

  @Nullable
  public CqlIdentifier getFieldToAlter() {
    return fieldToAlter;
  }

  @Nullable
  public DataType getFieldToAlterType() {
    return fieldToAlterType;
  }
}
