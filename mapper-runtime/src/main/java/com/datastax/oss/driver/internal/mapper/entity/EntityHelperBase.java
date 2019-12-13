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
package com.datastax.oss.driver.internal.mapper.entity;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.mapper.MapperBuilder;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.MapperException;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class EntityHelperBase<EntityT> implements EntityHelper<EntityT> {

  protected final CqlIdentifier keyspaceId;

  protected final CqlIdentifier tableId;

  protected final MapperContext context;

  protected EntityHelperBase(MapperContext context, String defaultTableName) {
    this(context, null, defaultTableName);
  }

  protected EntityHelperBase(
      MapperContext context, String defaultKeyspaceName, String defaultTableName) {
    this.context = context;
    this.tableId =
        context.getTableId() != null
            ? context.getTableId()
            : CqlIdentifier.fromCql(defaultTableName);
    this.keyspaceId =
        context.getKeyspaceId() != null
            ? context.getKeyspaceId()
            : (defaultKeyspaceName == null ? null : CqlIdentifier.fromCql(defaultKeyspaceName));
  }

  @Nullable
  @Override
  public CqlIdentifier getKeyspaceId() {
    return keyspaceId;
  }

  @NonNull
  @Override
  public CqlIdentifier getTableId() {
    return tableId;
  }

  protected void throwIfKeyspaceMissing() {
    if (this.getKeyspaceId() == null && !context.getSession().getKeyspace().isPresent()) {
      throw new MapperException(
          String.format(
              "Missing keyspace. Suggestions: use SessionBuilder.withKeyspace() "
                  + "when creating your session, specify a default keyspace on %s with @%s"
                  + "(defaultKeyspace), or use a @%s method with a @%s parameter",
              this.getEntityClass().getSimpleName(),
              Entity.class.getSimpleName(),
              DaoFactory.class.getSimpleName(),
              DaoKeyspace.class.getSimpleName()));
    }
  }

  public List<CqlIdentifier> findMissingColumns(
      List<CqlIdentifier> expected, Collection<ColumnMetadata> actual) {
    List<CqlIdentifier> actualCql =
        actual.stream().map(ColumnMetadata::getName).collect(Collectors.toList());
    return findMissingColumnsCql(expected, actualCql);
  }

  public List<CqlIdentifier> findMissingColumnsCql(
      List<CqlIdentifier> expected, Collection<CqlIdentifier> actual) {
    List<CqlIdentifier> missingColumns = new ArrayList<>();
    for (CqlIdentifier cqlIdentifier : expected) {
      if (!actual.contains(cqlIdentifier)) {
        missingColumns.add(cqlIdentifier);
      }
    }
    return missingColumns;
  }

  /**
   * When the new instance of a class annotated with {@link Dao} is created an automatic check for
   * schema validation is performed. It verifies if all {@link Dao} entity fields are present in CQL
   * table. If not the {@link IllegalArgumentException} exception with detailed message is thrown.
   * This check has startup overhead so once your app is stable you may want to disable it. The
   * schema validation check is enabled by default. It can be disabled using the {@link
   * MapperBuilder#withSchemaValidationEnabled(boolean)} method.
   */
  public abstract void validateEntityFields();

  public List<String> findMissingTypes(
      Map<CqlIdentifier, GenericType<?>> expected,
      Map<CqlIdentifier, ColumnMetadata> actual,
      CodecRegistry codecRegistry) {
    List<String> missingCodecs = new ArrayList<>();

    for (Map.Entry<CqlIdentifier, GenericType<?>> expectedEntry : expected.entrySet()) {
      ColumnMetadata columnMetadata = actual.get(expectedEntry.getKey());
      try {
        codecRegistry.codecFor(columnMetadata.getType(), expectedEntry.getValue());
      } catch (CodecNotFoundException exception) {
        missingCodecs.add(
            String.format(
                "Field: %s, Entity Type: %s, CQL table type: %s",
                expectedEntry.getKey(), exception.getJavaType(), exception.getCqlType()));
      }
    }
    return missingCodecs;
  }

  public void throwMissingTypesIfNotEmpty(
      List<String> missingTypes,
      CqlIdentifier keyspaceId,
      CqlIdentifier tableId,
      String entityClassName) {
    if (!missingTypes.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "The CQL ks.table: %s.%s defined in the entity class: %s has wrong types:\n%s",
              keyspaceId, tableId, entityClassName, String.join("\n", missingTypes)));
    }
  }
}
