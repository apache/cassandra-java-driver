package com.datastax.driver.mapping;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

import com.datastax.driver.core.ConsistencyLevel;
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

    public static <T> EntityMapper<T> parseEntity(Class<T> entityClass, EntityMapper.Factory factory) {
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
            if (field.getAnnotation(Transcient.class) != null)
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

        mapper.addColumns(convert(pks, factory, mapper.entityClass),
                          convert(ccs, factory, mapper.entityClass),
                          convert(rgs, factory, mapper.entityClass));
        return mapper;
    }

    private static <T> List<ColumnMapper<T>> convert(List<Field> fields, EntityMapper.Factory factory, Class<T> klass) {
        List<ColumnMapper<T>> mappers = new ArrayList<ColumnMapper<T>>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            int pos = position(field);
            mappers.add(factory.createColumnMapper(klass, field, pos < 0 ? i : pos));
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
        if (column == null)
            return field.getName();

        return column.caseSensitive() ? column.name() : column.name().toLowerCase();
    }

    public static <T> AccessorMapper<T> parseAccessor(Class<T> accClass, AccessorMapper.Factory factory) {
        Accessor acc = accClass.getAnnotation(Accessor.class);
        if (acc == null)
            throw new IllegalArgumentException("@Accessor annotation was not found on interface " + accClass.getName());

        String ksName = acc.caseSensitiveKeyspace() ? acc.keyspace() : acc.keyspace().toLowerCase();
        String tableName = acc.caseSensitiveTable() ? acc.name() : acc.name().toLowerCase();

        List<MethodMapper> methods = new ArrayList<MethodMapper>();
        for (Method m : accClass.getDeclaredMethods()) {
            Query query = m.getAnnotation(Query.class);
            if (query == null)
                continue;

            String queryString = query.value();

            Annotation[][] paramAnnotations = m.getParameterAnnotations();
            String[] paramNames = new String[paramAnnotations.length];
            outer:
            for (int i = 0; i < paramNames.length; i++) {
                for (Annotation a : paramAnnotations[i]) {
                    if (a.annotationType().equals(Param.class)) {
                       paramNames[i] = ((Param)a).value();
                       continue outer;
                    }
                }
                paramNames[i] = "arg" + i;
            }

            ConsistencyLevel cl = null;
            int fetchSize = -1;
            boolean tracing = false;

            QueryOptions options = m.getAnnotation(QueryOptions.class);
            if (options != null) {
                cl = options.consistency().isEmpty() ? null : ConsistencyLevel.valueOf(options.consistency().toUpperCase());
                fetchSize = options.fetchSize();
                tracing = options.tracing();
            }

            methods.add(new MethodMapper(m, queryString, paramNames, cl, fetchSize, tracing));
        }

        return factory.create(accClass, ksName, tableName, methods);
    }
}
