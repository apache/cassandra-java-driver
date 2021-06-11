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
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.AccessibleByName;
import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.api.core.data.SettableByName;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
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
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.internal.core.util.CollectionsUtils;
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

  @NonNull
  @Override
  @Deprecated
  public <SettableT extends SettableByName<SettableT>> SettableT set(
      @NonNull EntityT entity,
      @NonNull SettableT target,
      @NonNull NullSavingStrategy nullSavingStrategy) {
    return set(entity, target, nullSavingStrategy, false);
  }

  @NonNull
  @Override
  @Deprecated
  public EntityT get(@NonNull GettableByName source) {
    return get(source, false);
  }

  public void throwIfKeyspaceMissing() {
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
      List<CqlIdentifier> entityColumns, Collection<ColumnMetadata> cqlColumns) {
    return findMissingCqlIdentifiers(
        entityColumns,
        cqlColumns.stream().map(ColumnMetadata::getName).collect(Collectors.toList()));
  }

  public List<CqlIdentifier> findMissingCqlIdentifiers(
      List<CqlIdentifier> entityColumns, Collection<CqlIdentifier> cqlColumns) {
    List<CqlIdentifier> missingColumns = new ArrayList<>();
    for (CqlIdentifier entityCqlIdentifier : entityColumns) {
      if (!cqlColumns.contains(entityCqlIdentifier)) {
        missingColumns.add(entityCqlIdentifier);
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

  public static List<String> findTypeMismatches(
      Map<CqlIdentifier, GenericType<?>> entityColumns,
      Map<CqlIdentifier, ColumnMetadata> cqlColumns,
      CodecRegistry codecRegistry) {
    Map<CqlIdentifier, DataType> cqlColumnsDataTypes =
        cqlColumns.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    cqlIdentifierColumnMetadataEntry ->
                        cqlIdentifierColumnMetadataEntry.getValue().getType()));

    return findDataTypeMismatches(entityColumns, cqlColumnsDataTypes, codecRegistry);
  }

  public static List<String> findTypeMismatches(
      Map<CqlIdentifier, GenericType<?>> entityColumns,
      List<CqlIdentifier> cqlColumns,
      List<DataType> cqlTypes,
      CodecRegistry codecRegistry) {
    return findDataTypeMismatches(
        entityColumns,
        CollectionsUtils.combineListsIntoOrderedMap(cqlColumns, cqlTypes),
        codecRegistry);
  }

  private static List<String> findDataTypeMismatches(
      Map<CqlIdentifier, GenericType<?>> entityColumns,
      Map<CqlIdentifier, DataType> cqlColumns,
      CodecRegistry codecRegistry) {
    List<String> missingCodecs = new ArrayList<>();

    for (Map.Entry<CqlIdentifier, GenericType<?>> entityEntry : entityColumns.entrySet()) {
      DataType datType = cqlColumns.get(entityEntry.getKey());
      if (datType == null) {
        // this will not happen because it will be catch by the generateMissingColumnsCheck() method
        throw new AssertionError(
            "There is no cql column for entity column: " + entityEntry.getKey());
      }
      try {
        codecRegistry.codecFor(datType, entityEntry.getValue());
      } catch (CodecNotFoundException exception) {
        missingCodecs.add(
            String.format(
                "Field: %s, Entity Type: %s, CQL type: %s",
                entityEntry.getKey(), exception.getJavaType(), exception.getCqlType()));
      }
    }
    return missingCodecs;
  }

  public void throwMissingUdtTypesIfNotEmpty(
      List<String> missingTypes,
      CqlIdentifier keyspaceId,
      CqlIdentifier tableId,
      String entityClassName) {
    throwMissingTypesIfNotEmpty(missingTypes, keyspaceId, tableId, entityClassName, "udt");
  }

  public void throwMissingTableTypesIfNotEmpty(
      List<String> missingTypes,
      CqlIdentifier keyspaceId,
      CqlIdentifier tableId,
      String entityClassName) {
    throwMissingTypesIfNotEmpty(missingTypes, keyspaceId, tableId, entityClassName, "table");
  }

  public void throwMissingTypesIfNotEmpty(
      List<String> missingTypes,
      CqlIdentifier keyspaceId,
      CqlIdentifier tableId,
      String entityClassName,
      String type) {
    if (!missingTypes.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "The CQL ks.%s: %s.%s defined in the entity class: %s declares type mappings that are not supported by the codec registry:\n%s",
              type, keyspaceId, tableId, entityClassName, String.join("\n", missingTypes)));
    }
  }

  public boolean keyspaceNamePresent(
      Map<CqlIdentifier, KeyspaceMetadata> keyspaces, CqlIdentifier keyspaceId) {
    return keyspaces.containsKey(keyspaceId);
  }

  public boolean hasProperty(AccessibleByName source, String name) {
    if (source instanceof Row) {
      return ((Row) source).getColumnDefinitions().contains(name);
    } else if (source instanceof UdtValue) {
      return ((UdtValue) source).getType().contains(name);
    } else if (source instanceof BoundStatement) {
      return ((BoundStatement) source)
          .getPreparedStatement()
          .getVariableDefinitions()
          .contains(name);
    } else if (source instanceof BoundStatementBuilder) {
      return ((BoundStatementBuilder) source)
          .getPreparedStatement()
          .getVariableDefinitions()
          .contains(name);
    }
    // other implementations: assume the property is present
    return true;
  }
}
