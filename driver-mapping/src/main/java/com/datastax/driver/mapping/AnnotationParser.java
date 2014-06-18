package com.datastax.driver.mapping;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

import com.datastax.driver.core.ConsistencyLevel;

import com.datastax.driver.mapping.MethodMapper.ParamMapper;
import com.datastax.driver.mapping.MethodMapper.UDTListParamMapper;
import com.datastax.driver.mapping.MethodMapper.UDTMapParamMapper;
import com.datastax.driver.mapping.MethodMapper.UDTParamMapper;
import com.datastax.driver.mapping.MethodMapper.UDTSetParamMapper;
import com.datastax.driver.mapping.annotations.*;

/**
 * Static metods that facilitates parsing class annotations into the corresponding {@link EntityMapper}.
 */
class AnnotationParser {

    private static final Comparator<Field> fieldComparator = new Comparator<Field>() {
        public int compare(Field f1, Field f2) {
            return position(f2) - position(f1);
        }
    };

    private AnnotationParser() {}

    public static <T> EntityMapper<T> parseEntity(Class<T> entityClass, EntityMapper.Factory factory, MappingManager mappingManager) {
        Table table = entityClass.getAnnotation(Table.class);
        if (table == null)
            throw new IllegalArgumentException("@Table annotation was not found on class " + entityClass.getName());

        String ksName = table.caseSensitiveKeyspace() ? table.keyspace() : table.keyspace().toLowerCase();
        String tableName = table.caseSensitiveTable() ? table.name() : table.name().toLowerCase();

        ConsistencyLevel writeConsistency = table.writeConsistency().isEmpty() ? null : ConsistencyLevel.valueOf(table.writeConsistency().toUpperCase());
        ConsistencyLevel readConsistency = table.readConsistency().isEmpty() ? null : ConsistencyLevel.valueOf(table.readConsistency().toUpperCase());

        EntityMapper<T> mapper = factory.create(entityClass, ksName, tableName, writeConsistency, readConsistency);

        List<Field> pks = new ArrayList<Field>();
        List<Field> ccs = new ArrayList<Field>();
        List<Field> rgs = new ArrayList<Field>();

        for (Field field : entityClass.getDeclaredFields()) {
            if (field.getAnnotation(Transient.class) != null)
                continue;

            switch (kind(field)) {
                case PARTITION_KEY:
                    pks.add(field);
                    break;
                case CLUSTERING_COLUMN:
                    ccs.add(field);
                    break;
                default:
                    rgs.add(field);
                    break;
            }
        }

        Collections.sort(pks, fieldComparator);
        Collections.sort(ccs, fieldComparator);

        validateOrder(pks, "@PartitionKey");
        validateOrder(ccs, "@ClusteringColumn");

        mapper.addColumns(convert(pks, factory, mapper.entityClass, mappingManager),
                          convert(ccs, factory, mapper.entityClass, mappingManager),
                          convert(rgs, factory, mapper.entityClass, mappingManager));
        return mapper;
    }

    public static <T> EntityMapper<T> parseNested(Class<T> nestedClass, EntityMapper.Factory factory, MappingManager mappingManager) {
        UDT udt = nestedClass.getAnnotation(UDT.class);
        if (udt == null)
            throw new IllegalArgumentException(String.format("@%s annotation was not found on class %s", UDT.class.getSimpleName(), nestedClass.getName()));

        String ksName = udt.caseSensitiveKeyspace() ? udt.keyspace() : udt.keyspace().toLowerCase();
        String udtName = udt.caseSensitiveType() ? udt.name() : udt.name().toLowerCase();

        EntityMapper<T> mapper = factory.create(nestedClass, ksName, udtName, null, null);

        List<Field> columns = new ArrayList<Field>();

        for (Field field : nestedClass.getDeclaredFields()) {
            if (field.getAnnotation(Transient.class) != null)
                continue;

            switch (kind(field)) {
                case PARTITION_KEY:
                    throw new IllegalArgumentException("Annotation @PartitionKey is not allowed in a nested class");
                case CLUSTERING_COLUMN:
                    throw new IllegalArgumentException("Annotation @ClusteringColumn is not allowed in a nested class");
                default:
                    columns.add(field);
                    break;
            }
        }

        mapper.addColumns(convert(columns, factory, nestedClass, mappingManager));
        return mapper;
    }

    private static <T> List<ColumnMapper<T>> convert(List<Field> fields, EntityMapper.Factory factory, Class<T> klass, MappingManager mappingManager) {
        List<ColumnMapper<T>> mappers = new ArrayList<ColumnMapper<T>>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            int pos = position(field);
            mappers.add(factory.createColumnMapper(klass, field, pos < 0 ? i : pos, mappingManager));
        }
        return mappers;
    }

    private static void validateOrder(List<Field> fields, String annotation) {
        for (int i = 0; i < fields.size(); i++) {
            int pos = position(fields.get(i));
            if (pos < i)
                throw new IllegalArgumentException("Missing ordering value " + i + " for " + annotation + " annotation");
            else if (pos > i)
                throw new IllegalArgumentException("Duplicate ordering value " + i + " for " + annotation + " annotation");
        }
    }

    private static int position(Field field) {
        switch (kind(field)) {
            case PARTITION_KEY:
                return field.getAnnotation(PartitionKey.class).value();
            case CLUSTERING_COLUMN:
                return field.getAnnotation(ClusteringColumn.class).value();
            default:
                return -1;
        }
    }

    public static ColumnMapper.Kind kind(Field field) {
        PartitionKey pk = field.getAnnotation(PartitionKey.class);
        ClusteringColumn cc = field.getAnnotation(ClusteringColumn.class);
        if (pk != null && cc != null)
            throw new IllegalArgumentException("Field " + field.getName() + " cannot have both the @PartitionKey and @ClusteringColumn annotations");

        return pk != null
             ? ColumnMapper.Kind.PARTITION_KEY
             : (cc != null ? ColumnMapper.Kind.CLUSTERING_COLUMN : ColumnMapper.Kind.REGULAR);
    }

    public static EnumType enumType(Field field) {
        Class<?> type = field.getType();
        if (!type.isEnum())
            return null;

        Enumerated enumerated = field.getAnnotation(Enumerated.class);
        return (enumerated == null) ? EnumType.STRING : enumerated.value();
    }

    public static String columnName(Field field) {
        Column column = field.getAnnotation(Column.class);
        if (column != null) {
            return column.caseSensitive() ? column.name() : column.name().toLowerCase();
        }

        com.datastax.driver.mapping.annotations.Field udtField = field.getAnnotation(com.datastax.driver.mapping.annotations.Field.class);
        if (udtField != null) {
            return udtField.caseSensitive() ? udtField.name() : udtField.name().toLowerCase();
        }

        return field.getName();
    }

    public static <T> AccessorMapper<T> parseAccessor(Class<T> accClass, AccessorMapper.Factory factory, MappingManager mappingManager) {
        if (!accClass.isInterface())
            throw new IllegalArgumentException("@Accessor annotation is only allowed on interfaces");

        Accessor acc = accClass.getAnnotation(Accessor.class);
        if (acc == null)
            throw new IllegalArgumentException("@Accessor annotation was not found on interface " + accClass.getName());

        List<MethodMapper> methods = new ArrayList<MethodMapper>();
        for (Method m : accClass.getDeclaredMethods()) {
            Query query = m.getAnnotation(Query.class);
            if (query == null)
                continue;

            String queryString = query.value();

            Annotation[][] paramAnnotations = m.getParameterAnnotations();
            Type[] paramTypes = m.getGenericParameterTypes();
            ParamMapper[] paramMappers = new ParamMapper[paramAnnotations.length];
            for (int i = 0; i < paramMappers.length; i++) {
                String paramName = null;
                for (Annotation a : paramAnnotations[i]) {
                    if (a.annotationType().equals(Param.class)) {
                        paramName = ((Param) a).value();
                        break;
                    }
                }
                if (paramName == null) {
                    paramName = "arg" + i;
                }
                paramMappers[i] = newParamMapper(accClass.getName(), m.getName(), paramName, paramTypes[i], paramAnnotations[i], mappingManager);
            }

            ConsistencyLevel cl = null;
            int fetchSize = -1;
            boolean tracing = false;

            QueryParameters options = m.getAnnotation(QueryParameters.class);
            if (options != null) {
                cl = options.consistency().isEmpty() ? null : ConsistencyLevel.valueOf(options.consistency().toUpperCase());
                fetchSize = options.fetchSize();
                tracing = options.tracing();
            }

            methods.add(new MethodMapper(m, queryString, paramMappers, cl, fetchSize, tracing));
        }

        return factory.create(accClass, methods);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static ParamMapper newParamMapper(String className, String methodName, String paramName, Type paramType, Annotation[] paramAnnotations, MappingManager mappingManager) {
        // TODO support enums

        if (paramType instanceof Class) {
            Class<?> paramClass = (Class<?>) paramType;
            if (paramClass.isAnnotationPresent(UDT.class)) {
                NestedMapper<?> nestedMapper = mappingManager.getNestedMapper(paramClass);
                return new UDTParamMapper(paramName, nestedMapper);
            }
            return new ParamMapper(paramName);
        } if (paramType instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType) paramType;
            Type raw = pt.getRawType();
            if (!(raw instanceof Class))
                throw new IllegalArgumentException(String.format("Cannot map class %s for parameter %s of %s.%s", paramType, paramName, className, methodName));
            Class<?> klass = (Class<?>)raw;
            Class<?> firstTypeParam = ReflectionUtils.getParam(pt, 0, paramName);
            if (List.class.isAssignableFrom(klass) && firstTypeParam.isAnnotationPresent(UDT.class)) {
                NestedMapper<?> valueMapper = mappingManager.getNestedMapper(firstTypeParam);
                return new UDTListParamMapper(paramName, valueMapper);
            }
            if (Set.class.isAssignableFrom(klass) && firstTypeParam.isAnnotationPresent(UDT.class)) {
                NestedMapper<?> valueMapper = mappingManager.getNestedMapper(firstTypeParam);
                return new UDTSetParamMapper(paramName, valueMapper);
            }
            if (Map.class.isAssignableFrom(klass)) {
                Class<?> secondTypeParam = ReflectionUtils.getParam(pt, 1, paramName);
                NestedMapper<?> keyMapper = firstTypeParam.isAnnotationPresent(UDT.class) ? mappingManager.getNestedMapper(firstTypeParam) : null;
                NestedMapper<?> valueMapper = secondTypeParam.isAnnotationPresent(UDT.class) ? mappingManager.getNestedMapper(secondTypeParam) : null;
                if (keyMapper != null || valueMapper != null) {
                    return new UDTMapParamMapper(paramName, keyMapper, valueMapper);
                }
            }
            return new ParamMapper(paramName);
        } else {
            throw new IllegalArgumentException(String.format("Cannot map class %s for parameter %s of %s.%s", paramType, paramName, className, methodName));
        }
    }
}
