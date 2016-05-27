/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.mapping;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UserType;
import com.datastax.driver.mapping.MethodMapper.ParamMapper;
import com.datastax.driver.mapping.annotations.*;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import java.beans.BeanInfo;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.datastax.driver.core.Metadata.quote;

/**
 * Static methods that facilitates parsing class annotations into the corresponding {@link EntityMapper}.
 */
@SuppressWarnings("unchecked")
class AnnotationParser {

    private AnnotationParser() {
    }

    static <T> EntityMapper<T> parseEntity(final Class<T> entityClass, EntityMapper.Factory factory, MappingManager mappingManager) {
        Table table = AnnotationChecks.getTypeAnnotation(Table.class, entityClass);

        String ksName = table.caseSensitiveKeyspace() ? table.keyspace() : table.keyspace().toLowerCase();
        String tableName = table.caseSensitiveTable() ? table.name() : table.name().toLowerCase();

        ConsistencyLevel writeConsistency = table.writeConsistency().isEmpty() ? null : ConsistencyLevel.valueOf(table.writeConsistency().toUpperCase());
        ConsistencyLevel readConsistency = table.readConsistency().isEmpty() ? null : ConsistencyLevel.valueOf(table.readConsistency().toUpperCase());

        if (Strings.isNullOrEmpty(table.keyspace())) {
            ksName = mappingManager.getSession().getLoggedKeyspace();
            if (Strings.isNullOrEmpty(ksName))
                throw new IllegalArgumentException(String.format(
                        "Error creating mapper for class %s, the @%s annotation declares no default keyspace, and the session is not currently logged to any keyspace",
                        entityClass.getSimpleName(),
                        Table.class.getSimpleName()
                ));
        }

        EntityMapper<T> mapper = factory.create(entityClass, ksName, tableName, writeConsistency, readConsistency);
        TableMetadata tableMetadata = mappingManager.getSession().getCluster().getMetadata().getKeyspace(ksName).getTable(tableName);

        if (tableMetadata == null)
            throw new IllegalArgumentException(String.format("Table %s does not exist in keyspace %s", tableName, ksName));

        List<MappedProperty<T>> pks = new ArrayList<MappedProperty<T>>();
        List<MappedProperty<T>> ccs = new ArrayList<MappedProperty<T>>();
        List<MappedProperty<T>> rgs = new ArrayList<MappedProperty<T>>();

        BeanInfo beanInfo = ReflectionUtils.getBeanInfo(entityClass);

        for (PropertyDescriptor descriptor : beanInfo.getPropertyDescriptors()) {

            if (descriptor.getName().equals("class"))
                continue;

            MappedProperty<T> property = new MappedProperty<T>(descriptor, entityClass);

            if (mappingManager.isCassandraV1 && property.isComputed())
                throw new UnsupportedOperationException("Computed properties are not supported with native protocol v1");

            AnnotationChecks.validateAnnotations(property,
                    Column.class, ClusteringColumn.class, Frozen.class, FrozenKey.class,
                    FrozenValue.class, PartitionKey.class, Transient.class, Computed.class);

            if (property.isTransient())
                continue;

            switch (property.kind()) {
                case PARTITION_KEY:
                    pks.add(property);
                    break;
                case CLUSTERING_COLUMN:
                    ccs.add(property);
                    break;
                default:
                    rgs.add(property);
                    break;
            }
        }

        AtomicInteger columnCounter = mappingManager.isCassandraV1 ? null : new AtomicInteger(0);

        Collections.sort(pks);
        Collections.sort(ccs);

        AnnotationChecks.validateOrder(pks, "@PartitionKey");
        AnnotationChecks.validateOrder(ccs, "@ClusteringColumn");

        mapper.addColumns(
                createColumnMappers(pks, factory, mappingManager, columnCounter, tableMetadata, ksName, tableName),
                createColumnMappers(ccs, factory, mappingManager, columnCounter, tableMetadata, ksName, tableName),
                createColumnMappers(rgs, factory, mappingManager, columnCounter, tableMetadata, ksName, tableName));
        return mapper;
    }

    private static <T> List<ColumnMapper<T>> createColumnMappers(List<MappedProperty<T>> properties, EntityMapper.Factory factory, MappingManager mappingManager, AtomicInteger columnCounter, TableMetadata tableMetadata, String ksName, String tableName) {
        List<ColumnMapper<T>> mappers = new ArrayList<ColumnMapper<T>>(properties.size());
        for (int i = 0; i < properties.size(); i++) {
            MappedProperty<T> property = properties.get(i);
            int pos = property.position();
            ColumnMapper<T> columnMapper = factory.createColumnMapper(property, pos < 0 ? i : pos, mappingManager, columnCounter);
            if (property.isComputed() || tableMetadata.getColumn(columnMapper.getColumnName()) != null)
                mappers.add(columnMapper);
            else
                throw new IllegalArgumentException(String.format("Column %s does not exist in table %s.%s",
                        columnMapper.getColumnName(), ksName, tableName));
        }
        return mappers;
    }

    static <T> MappedUDTCodec<T> parseUDT(Class<T> udtClass, EntityMapper.Factory factory, MappingManager mappingManager) {
        UDT udt = AnnotationChecks.getTypeAnnotation(UDT.class, udtClass);

        String ksName = udt.caseSensitiveKeyspace() ? udt.keyspace() : udt.keyspace().toLowerCase();
        String udtName = udt.caseSensitiveType() ? quote(udt.name()) : udt.name().toLowerCase();

        if (Strings.isNullOrEmpty(udt.keyspace())) {
            ksName = mappingManager.getSession().getLoggedKeyspace();
            if (Strings.isNullOrEmpty(ksName))
                throw new IllegalArgumentException(String.format(
                        "Error creating UDT codec for class %s, the @%s annotation declares no default keyspace, and the session is not currently logged to any keyspace",
                        udtClass.getSimpleName(),
                        UDT.class.getSimpleName()
                ));
        }

        UserType userType = mappingManager.getSession().getCluster().getMetadata().getKeyspace(ksName).getUserType(udtName);
        if (userType == null)
            throw new IllegalArgumentException(String.format("User type %s does not exist in keyspace %s", udtName, ksName));

        List<MappedProperty<T>> columns = new ArrayList<MappedProperty<T>>();

        BeanInfo beanInfo = ReflectionUtils.getBeanInfo(udtClass);

        for (PropertyDescriptor descriptor : beanInfo.getPropertyDescriptors()) {

            if (descriptor.getName().equals("class"))
                continue;

            MappedProperty<T> property = new MappedProperty<T>(descriptor, udtClass);

            AnnotationChecks.validateAnnotations(property,
                    Field.class, Frozen.class, FrozenKey.class,
                    FrozenValue.class, Transient.class);

            if (property.isTransient())
                continue;

            AnnotationChecks.validatePrimaryKeyOnUDT(property);
            columns.add(property);
        }
        Map<String, ColumnMapper<T>> columnMappers = createFieldMappers(columns, factory, udtClass, mappingManager, userType, ksName);
        return new MappedUDTCodec<T>(userType, udtClass, columnMappers, mappingManager);
    }

    private static <T> Map<String, ColumnMapper<T>> createFieldMappers(List<MappedProperty<T>> properties, EntityMapper.Factory factory, Class<T> udtClass, MappingManager mappingManager, UserType userType, String ksName) {
        Map<String, ColumnMapper<T>> mappers = Maps.newHashMapWithExpectedSize(properties.size());
        for (int i = 0; i < properties.size(); i++) {
            MappedProperty<T> property = properties.get(i);
            int pos = property.position();
            ColumnMapper<T> mapper = factory.createColumnMapper(property, pos < 0 ? i : pos, mappingManager, null);
            if (userType.contains(mapper.getColumnName()))
                mappers.put(mapper.getColumnName(), mapper);
            else
                throw new IllegalArgumentException(String.format("Field %s does not exist in type %s.%s",
                        mapper.getColumnName(), ksName, userType.getTypeName()));
        }
        return mappers;
    }


    static String newAlias(int columnNumber) {
        return "col" + columnNumber;

    }


    static <T> AccessorMapper<T> parseAccessor(Class<T> accClass, AccessorMapper.Factory factory, MappingManager mappingManager) {
        if (!accClass.isInterface())
            throw new IllegalArgumentException("@Accessor annotation is only allowed on interfaces");

        AnnotationChecks.getTypeAnnotation(Accessor.class, accClass);

        List<MethodMapper> methods = new ArrayList<MethodMapper>();
        for (Method m : accClass.getDeclaredMethods()) {
            Query query = m.getAnnotation(Query.class);
            if (query == null)
                continue;

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
                        if (paramName.isEmpty())
                            paramName = null;
                        codecClass = param.codec();
                        if (Defaults.NoCodec.class.equals(codecClass))
                            codecClass = null;
                        break;
                    }
                }
                boolean thisParamNamed = (paramName != null);
                if (allParamsNamed == null)
                    allParamsNamed = thisParamNamed;
                else if (allParamsNamed != thisParamNamed)
                    throw new IllegalArgumentException(String.format("For method '%s', either all or none of the parameters must be named", m.getName()));

                paramMappers[i] = newParamMapper(accClass.getName(), m.getName(), i, paramName, codecClass, paramTypes[i], mappingManager);
            }

            ConsistencyLevel cl = null;
            int fetchSize = -1;
            boolean tracing = false;
            Boolean idempotent = null;

            QueryParameters options = m.getAnnotation(QueryParameters.class);
            if (options != null) {
                cl = options.consistency().isEmpty() ? null : ConsistencyLevel.valueOf(options.consistency().toUpperCase());
                fetchSize = options.fetchSize();
                tracing = options.tracing();
                if (options.idempotent().length > 1) {
                    throw new IllegalStateException("idemtpotence() attribute can only accept one value");
                }
                idempotent = options.idempotent().length == 0 ? null : options.idempotent()[0];
            }

            methods.add(new MethodMapper(m, queryString, paramMappers, cl, fetchSize, tracing, idempotent));
        }

        return factory.create(accClass, methods);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static ParamMapper newParamMapper(String className, String methodName, int idx, String paramName, Class<? extends TypeCodec<?>> codecClass, Type paramType, MappingManager mappingManager) {
        if (paramType instanceof Class) {
            Class<?> paramClass = (Class<?>) paramType;
            if (TypeMappings.isMappedUDT(paramClass))
                mappingManager.getUDTCodec(paramClass);

            return new ParamMapper(paramName, idx, TypeToken.of(paramType), codecClass);
        } else if (paramType instanceof ParameterizedType) {
            for (Class<?> udt : TypeMappings.findUDTs(paramType))
                mappingManager.getUDTCodec(udt);

            return new ParamMapper(paramName, idx, TypeToken.of(paramType), codecClass);
        } else {
            throw new IllegalArgumentException(String.format("Cannot map class %s for parameter %s of %s.%s", paramType, paramName, className, methodName));
        }
    }
}
