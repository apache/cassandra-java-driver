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
package com.datastax.driver.mapping;

import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UserType;
import com.datastax.driver.mapping.MethodMapper.ParamMapper;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Defaults;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;
import com.datastax.driver.mapping.annotations.QueryParameters;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.UDT;
import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

class AnnotationParser {

  private static final Comparator<AliasedMappedProperty> POSITION_COMPARATOR =
      new Comparator<AliasedMappedProperty>() {
        @Override
        public int compare(AliasedMappedProperty o1, AliasedMappedProperty o2) {
          return o1.mappedProperty.getPosition() - o2.mappedProperty.getPosition();
        }
      };

  private AnnotationParser() {}

  static <T> EntityMapper<T> parseEntity(
      final Class<T> entityClass, String keyspaceOverride, MappingManager mappingManager) {
    Table table = AnnotationChecks.getTypeAnnotation(Table.class, entityClass);

    String keyspaceName;
    String loggedKeyspace = mappingManager.getSession().getLoggedKeyspace();
    if (!Strings.isNullOrEmpty(keyspaceOverride)) {
      keyspaceName = keyspaceOverride;
    } else if (!Strings.isNullOrEmpty(table.keyspace())) {
      keyspaceName =
          (table.caseSensitiveKeyspace()) ? Metadata.quote(table.keyspace()) : table.keyspace();
    } else if (!Strings.isNullOrEmpty(loggedKeyspace)) {
      keyspaceName = Metadata.quote(loggedKeyspace);
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Error creating mapper for %s, you must provide a keyspace name "
                  + "(either as an argument to the MappingManager.mapper() call, "
                  + "or via the @Table annotation, or by having a default keyspace on your Session)",
              entityClass));
    }

    String tableName =
        table.caseSensitiveTable() ? Metadata.quote(table.name()) : table.name().toLowerCase();

    ConsistencyLevel writeConsistency =
        table.writeConsistency().isEmpty()
            ? null
            : ConsistencyLevel.valueOf(table.writeConsistency().toUpperCase());
    ConsistencyLevel readConsistency =
        table.readConsistency().isEmpty()
            ? null
            : ConsistencyLevel.valueOf(table.readConsistency().toUpperCase());

    KeyspaceMetadata keyspaceMetadata =
        mappingManager.getSession().getCluster().getMetadata().getKeyspace(keyspaceName);
    if (keyspaceMetadata == null)
      throw new IllegalArgumentException(String.format("Keyspace %s does not exist", keyspaceName));

    AbstractTableMetadata tableMetadata = keyspaceMetadata.getTable(tableName);
    if (tableMetadata == null) {
      tableMetadata = keyspaceMetadata.getMaterializedView(tableName);
      if (tableMetadata == null)
        throw new IllegalArgumentException(
            String.format(
                "Table or materialized view %s does not exist in keyspace %s",
                tableName, keyspaceName));
    }

    EntityMapper<T> mapper =
        new EntityMapper<T>(
            entityClass, keyspaceName, tableName, writeConsistency, readConsistency);

    List<AliasedMappedProperty> pks = new ArrayList<AliasedMappedProperty>();
    List<AliasedMappedProperty> ccs = new ArrayList<AliasedMappedProperty>();
    List<AliasedMappedProperty> rgs = new ArrayList<AliasedMappedProperty>();

    MappingConfiguration configuration = mappingManager.getConfiguration();
    Set<? extends MappedProperty<?>> properties =
        configuration.getPropertyMapper().mapTable(entityClass);
    AtomicInteger columnCounter =
        mappingManager.protocolVersionAsInt == 1 ? null : new AtomicInteger(0);

    for (MappedProperty<?> mappedProperty : properties) {

      String alias = (columnCounter != null) ? "col" + columnCounter.incrementAndGet() : null;

      AliasedMappedProperty aliasedMappedProperty =
          new AliasedMappedProperty(mappedProperty, alias);

      if (mappingManager.protocolVersionAsInt == 1 && mappedProperty.isComputed())
        throw new UnsupportedOperationException(
            "Computed properties are not supported with native protocol v1");

      if (!mappedProperty.isComputed()
          && tableMetadata.getColumn(mappedProperty.getMappedName()) == null)
        throw new IllegalArgumentException(
            String.format(
                "Column %s does not exist in table %s.%s",
                mappedProperty.getMappedName(), keyspaceName, tableName));

      if (mappedProperty.isPartitionKey()) pks.add(aliasedMappedProperty);
      else if (mappedProperty.isClusteringColumn()) ccs.add(aliasedMappedProperty);
      else rgs.add(aliasedMappedProperty);

      // if the property is of a UDT type, parse it now
      for (Class<?> udt : TypeMappings.findUDTs(mappedProperty.getPropertyType().getType()))
        mappingManager.getUDTCodec(udt, keyspaceName);
    }

    Collections.sort(pks, POSITION_COMPARATOR);
    Collections.sort(ccs, POSITION_COMPARATOR);

    AnnotationChecks.validateOrder(pks, "@PartitionKey");
    AnnotationChecks.validateOrder(ccs, "@ClusteringColumn");

    mapper.addColumns(pks, ccs, rgs);
    return mapper;
  }

  static <T> MappedUDTCodec<T> parseUDT(
      Class<T> udtClass, String keyspaceOverride, MappingManager mappingManager) {
    UDT udt = AnnotationChecks.getTypeAnnotation(UDT.class, udtClass);

    String keyspaceName;
    String loggedKeyspace = mappingManager.getSession().getLoggedKeyspace();
    if (!Strings.isNullOrEmpty(keyspaceOverride)) {
      keyspaceName = keyspaceOverride;
    } else if (!Strings.isNullOrEmpty(udt.keyspace())) {
      keyspaceName =
          (udt.caseSensitiveKeyspace()) ? Metadata.quote(udt.keyspace()) : udt.keyspace();
    } else if (!Strings.isNullOrEmpty(loggedKeyspace)) {
      keyspaceName = Metadata.quote(loggedKeyspace);
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Error creating mapper for %s, you must provide a keyspace name "
                  + "(either as an argument to the MappingManager.mapper() call, "
                  + "or via the @Table annotation, or by having a default keyspace on your Session)",
              udtClass));
    }

    String udtName =
        udt.caseSensitiveType() ? Metadata.quote(udt.name()) : udt.name().toLowerCase();

    KeyspaceMetadata keyspaceMetadata =
        mappingManager.getSession().getCluster().getMetadata().getKeyspace(keyspaceName);
    if (keyspaceMetadata == null)
      throw new IllegalArgumentException(String.format("Keyspace %s does not exist", keyspaceName));

    UserType userType = keyspaceMetadata.getUserType(udtName);
    if (userType == null)
      throw new IllegalArgumentException(
          String.format("User type %s does not exist in keyspace %s", udtName, keyspaceName));

    Map<String, AliasedMappedProperty> propertyMappers =
        new HashMap<String, AliasedMappedProperty>();

    MappingConfiguration configuration = mappingManager.getConfiguration();
    Set<? extends MappedProperty<?>> properties =
        configuration.getPropertyMapper().mapUdt(udtClass);

    for (MappedProperty<?> mappedProperty : properties) {

      AliasedMappedProperty aliasedMappedProperty = new AliasedMappedProperty(mappedProperty, null);

      if (!userType.contains(mappedProperty.getMappedName()))
        throw new IllegalArgumentException(
            String.format(
                "Field %s does not exist in type %s.%s",
                mappedProperty.getMappedName(), keyspaceName, userType.getTypeName()));

      for (Class<?> fieldUdt : TypeMappings.findUDTs(mappedProperty.getPropertyType().getType()))
        mappingManager.getUDTCodec(fieldUdt, keyspaceName);

      propertyMappers.put(mappedProperty.getMappedName(), aliasedMappedProperty);
    }

    return new MappedUDTCodec<T>(userType, udtClass, propertyMappers, mappingManager);
  }

  static <T> AccessorMapper<T> parseAccessor(Class<T> accClass, MappingManager mappingManager) {
    if (!accClass.isInterface())
      throw new IllegalArgumentException(
          "@Accessor annotation is only allowed on interfaces, got " + accClass);

    AnnotationChecks.getTypeAnnotation(Accessor.class, accClass);

    List<MethodMapper> methods = new ArrayList<MethodMapper>();
    for (Method m : accClass.getDeclaredMethods()) {
      Query query = m.getAnnotation(Query.class);
      if (query == null) continue;

      String queryString = query.value();

      Annotation[][] paramAnnotations = m.getParameterAnnotations();
      Type[] paramTypes = m.getGenericParameterTypes();
      ParamMapper[] paramMappers = new ParamMapper[paramAnnotations.length];
      Boolean allParamsNamed = null;
      for (int i = 0; i < paramMappers.length; i++) {
        String paramName = null;
        Class<? extends TypeCodec<?>> codecClass = null;
        for (Annotation a : paramAnnotations[i]) {
          if (a.annotationType().equals(Param.class)) {
            Param param = (Param) a;
            paramName = param.value();
            if (paramName.isEmpty()) paramName = null;
            codecClass = param.codec();
            if (Defaults.NoCodec.class.equals(codecClass)) codecClass = null;
            break;
          }
        }
        boolean thisParamNamed = (paramName != null);
        if (allParamsNamed == null) allParamsNamed = thisParamNamed;
        else if (allParamsNamed != thisParamNamed)
          throw new IllegalArgumentException(
              String.format(
                  "For method '%s', either all or none of the parameters must be named",
                  m.getName()));

        paramMappers[i] =
            newParamMapper(
                accClass.getName(),
                m.getName(),
                i,
                paramName,
                codecClass,
                paramTypes[i],
                mappingManager);
      }

      ConsistencyLevel cl = null;
      int fetchSize = -1;
      boolean tracing = false;
      Boolean idempotent = null;

      QueryParameters options = m.getAnnotation(QueryParameters.class);
      if (options != null) {
        cl =
            options.consistency().isEmpty()
                ? null
                : ConsistencyLevel.valueOf(options.consistency().toUpperCase());
        fetchSize = options.fetchSize();
        tracing = options.tracing();
        if (options.idempotent().length > 1) {
          throw new IllegalArgumentException("idemtpotence() attribute can only accept one value");
        }
        idempotent = options.idempotent().length == 0 ? null : options.idempotent()[0];
      }

      methods.add(
          new MethodMapper(m, queryString, paramMappers, cl, fetchSize, tracing, idempotent));
    }

    return new AccessorMapper<T>(accClass, methods);
  }

  private static ParamMapper newParamMapper(
      String className,
      String methodName,
      int idx,
      String paramName,
      Class<? extends TypeCodec<?>> codecClass,
      Type paramType,
      MappingManager mappingManager) {
    if (paramType instanceof Class) {
      Class<?> paramClass = (Class<?>) paramType;
      if (TypeMappings.isMappedUDT(paramClass)) mappingManager.getUDTCodec(paramClass, null);

      return new ParamMapper(paramName, idx, TypeToken.of(paramType), codecClass);
    } else if (paramType instanceof ParameterizedType) {
      for (Class<?> udt : TypeMappings.findUDTs(paramType)) mappingManager.getUDTCodec(udt, null);

      return new ParamMapper(paramName, idx, TypeToken.of(paramType), codecClass);
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Cannot map class %s for parameter %s of %s.%s",
              paramType, paramName, className, methodName));
    }
  }
}
