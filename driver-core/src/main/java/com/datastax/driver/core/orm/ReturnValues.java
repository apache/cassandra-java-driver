package com.datastax.driver.core.orm;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Map;

import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.orm.mapping.CustomData;
import com.datastax.driver.core.orm.mapping.Customizable;

enum ReturnValues {
    INSTANCE;

    public ReturnValue factory(Field field) {
        if (ColumnUtil.INTANCE.isEnumField(field)) {
            return new EnumReturnValue();
        }
        if (ColumnUtil.INTANCE.isList(field)) {
            return new ListReturnValue();
        }
        if (ColumnUtil.INTANCE.isSet(field)) {
            return new SetReturnValue();
        }
        if (ColumnUtil.INTANCE.isMap(field)) {
            return new MapReturnValue();
        }
        if(ColumnUtil.INTANCE.isCustom(field)){
            return new CustomReturnValue();
        }

        return new DefaultGetObject();
    }

    
    class CustomReturnValue implements ReturnValue{

        @Override
        public Object getObject(Map<String, Definition> mapDefinition,Field field, Row row) {
            Definition column = mapDefinition.get(ColumnUtil.INTANCE.getColumnName(field).toLowerCase());
            ByteBuffer buffer= (ByteBuffer)RelationShipJavaCassandra.INSTANCE.getObject(row, column.getType().getName(), column.getName());
            CustomData customData=field.getAnnotation(CustomData.class);
            Customizable customizable=Customizable.class.cast(ReflectionUtil.INSTANCE.newInstance(customData.classCustmo()));
            
            return customizable.write(buffer);
        }
        
        
    }
    class MapReturnValue implements ReturnValue {

        @Override
        public Object getObject(Map<String, Definition> mapDefinition,Field field, Row row) {
        	ReflectionUtil.KeyValueClass keyValueClass=ReflectionUtil.INSTANCE.getGenericKeyValue(field);
            return RelationShipJavaCassandra.INSTANCE.getObject(row, Name.MAP,ColumnUtil.INTANCE.getColumnName(field),keyValueClass.getKeyClass(), keyValueClass.getValueClass());

        }

    }

    class SetReturnValue implements ReturnValue {

        @Override
        public Object getObject(Map<String, Definition> mapDefinition,Field field, Row row) {
            return RelationShipJavaCassandra.INSTANCE.getObject(row, Name.SET,ColumnUtil.INTANCE.getColumnName(field),ReflectionUtil.INSTANCE.getGenericType(field));

        }

    }

    class ListReturnValue implements ReturnValue {

        @Override
        public Object getObject(Map<String, Definition> mapDefinition,Field field, Row row) {
            return RelationShipJavaCassandra.INSTANCE.getObject(row, Name.LIST, ColumnUtil.INTANCE.getColumnName(field),ReflectionUtil.INSTANCE.getGenericType(field));

        }

    }

    class EnumReturnValue implements ReturnValue {

        @Override
        public Object getObject(Map<String, Definition> mapDefinition, Field field, Row row) {
            Integer value = (Integer) RelationShipJavaCassandra.INSTANCE.getObject(row, Name.INT,ColumnUtil.INTANCE.getColumnName(field));
            return field.getType().getEnumConstants()[value];
        }

    }

    class DefaultGetObject implements ReturnValue {

        @Override
        public Object getObject(Map<String, Definition> mapDefinition,Field field, Row row) {
            Definition column = mapDefinition.get(ColumnUtil.INTANCE.getColumnName(field).toLowerCase());
            return RelationShipJavaCassandra.INSTANCE.getObject(row, column.getType().getName(), column.getName());
        }

    }

    interface ReturnValue {
        Object getObject(Map<String, Definition> mapDefinition, Field field,Row row);
    }
}
