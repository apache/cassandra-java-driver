package com.datastax.driver.mapping;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.mapping.EntityDefinition.ColumnDefinition;
import com.datastax.driver.mapping.EntityDefinition.EnumColumnDefinition;
import com.datastax.driver.mapping.EntityDefinition.SubEntityDefinition;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.EnumMapping;
import com.datastax.driver.mapping.annotations.EnumValue;
import com.datastax.driver.mapping.annotations.Inheritance;
import com.datastax.driver.mapping.annotations.InheritanceValue;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.Transcient;

/**
 * Parses entities for Cassandra mapping annotation and build an {@link EntityDefinition} from it. 
 */
class AnnotationParser {

    public static <T> EntityDefinition<T> parseEntity(Class<T> entityClass) {

        EntityDefinition<T> entityDef = new EntityDefinition<T>(entityClass);

        // @Table
        Table table = entityClass.getAnnotation(Table.class);
        if (table == null) {
            throw new IllegalArgumentException("@Table annotation was not found on class " + entityClass.getName());
        }
        entityDef.tableName = table.name();
        entityDef.keyspaceName = table.keyspace();

        // @Inheritance
        Inheritance inheritance = entityClass.getAnnotation(Inheritance.class);
        if (inheritance != null) {
            entityDef.inheritanceColumn = inheritance.column();
            Map<String, SubEntityDefinition<T>> subEntities = new HashMap<String, SubEntityDefinition<T>>();
            for (Class<?> subClass : inheritance.subClasses()) {
                InheritanceValue inheritanceValue = subClass.getAnnotation(InheritanceValue.class);
                if (inheritanceValue == null) {
                    throw new IllegalArgumentException("Class " + subClass.getName() + " declared as subclass in @Inheritance annotation on "
                            + entityClass.getName() + " but is not annotated with @InheritanceValue.");
                }
                if (!entityClass.isAssignableFrom(subClass)) {
                    throw new IllegalArgumentException("Class " + subClass.getName() + " declared as subclass in @Inheritance annotation on "
                            + entityClass.getName() + " but is not an actual subclass.");
                }
                if (subEntities.containsKey(inheritanceValue.value())) {
                    Class<?> conflictingClass = subEntities.get(inheritanceValue.value()).subEntityClass;
                    throw new IllegalArgumentException(subClass.getName() + " and " + conflictingClass.getName()
                            + " both define '" + inheritanceValue.value() + "' as value in their @InheritanceValue annotation");
                }
                SubEntityDefinition<T> subEntityDef = parseSubEntity((Class<? extends T>)subClass, entityDef);
                subEntities.put(inheritanceValue.value(), subEntityDef);
            }
            entityDef.subEntities.addAll(subEntities.values());
        }

        for (Field field : entityClass.getDeclaredFields()) {
            Transcient transcient = field.getAnnotation(Transcient.class);
            if (transcient == null) {
                // Any field annotated as Transcient is ignored
                entityDef.columns.add(parseColumn(field));
            }
        }
        return entityDef;
    }

    private static <T> SubEntityDefinition<T> parseSubEntity(Class<? extends T> subEntityClass, EntityDefinition<T> entityDef) {

        SubEntityDefinition<T> subEntityDef = new SubEntityDefinition<T>(entityDef, subEntityClass);

        // @InheritanceValue
        InheritanceValue inheritanceValue = subEntityClass.getAnnotation(InheritanceValue.class);
        if (inheritanceValue == null) {
            throw new IllegalArgumentException("@InheritanceValue annotation was not found on class" + subEntityClass.getName());
        }
        subEntityDef.inheritanceColumnValue = inheritanceValue.value();
        for (Field field : subEntityClass.getDeclaredFields()) {
            Transcient transcient = field.getAnnotation(Transcient.class);
            if (transcient == null) {
                // Any field annotated as Transcient is ignored
                subEntityDef.columns.add(parseColumn(field));
            }
        }
        return subEntityDef;

    }

    private static ColumnDefinition parseColumn(Field field) {
        Class<?> type = field.getType();

        ColumnDefinition columnDef;
        if (type.isEnum()) {
            // Enum
            EnumColumnDefinition enumColumnDef = new EnumColumnDefinition();
            EnumMapping enumerated = field.getAnnotation(EnumMapping.class);
            EnumMappingType enumType = (enumerated == null) ? EnumMappingType.STRING : enumerated.value();
            for (int i = 0; i < type.getEnumConstants().length; i++) {
                Enum<?> en = (Enum<?>)type.getEnumConstants()[i];
                EnumValue enumValue;
                try {
                    enumValue = type.getField(en.name()).getAnnotation(EnumValue.class);
                } catch (Exception e) {
                    throw new IllegalStateException("Could not access element '" + en.name() + "' of enum " + type.getName());
                }
                String value;
                if (enumValue != null) {
                    enumColumnDef.hasCustomValues = true;
                    value = enumValue.value();
                } else {
                    value = (enumType == EnumMappingType.STRING) ? en.name() : Integer.toString(en.ordinal());
                }
                enumColumnDef.valueToEnum.put(value, en);
                enumColumnDef.enumToValue.put(en, value);
            }
            columnDef = enumColumnDef;
        } else {
            // Column
            columnDef = new ColumnDefinition();
        }

        Column column = field.getAnnotation(Column.class);
        String columnName = (column == null) ? field.getName() : column.name();
        columnDef.columnName = columnName;
        columnDef.javaType = type;
        columnDef.fieldName = field.getName();
        try {
            PropertyDescriptor pd = new PropertyDescriptor(columnDef.fieldName, field.getDeclaringClass());
            columnDef.readMethod = pd.getReadMethod();
            columnDef.writeMethod = pd.getWriteMethod();
        } catch (IntrospectionException e) {
            throw new RuntimeException("Can't find matching getter and setter for field '" + columnDef.fieldName + "'");
        }

        return columnDef;
    }
}
