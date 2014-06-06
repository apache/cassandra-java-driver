package com.datastax.driver.mapping;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.UDTDefinition;
import com.datastax.driver.core.UDTDefinition.Field;
import com.datastax.driver.core.UDTValue;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * An object handling the mapping of a class used as a field of another class.
 * <p>
 * This kind of class will be mapped to a User Defined Type.
 * </p>
 */
class NestedMapper<T> {
    // UDTs are always serialized with the v3 protocol
    private static final int UDT_PROTOCOL_VERSION = Math.max(ProtocolOptions.NEWEST_SUPPORTED_PROTOCOL_VERSION, 3);

    private final EntityMapper<T> entityMapper;
    private final UDTDefinition udtDefinition;

    NestedMapper(EntityMapper<T> entityMapper) {
        this.entityMapper = entityMapper;
        this.udtDefinition = new UDTDefinition(entityMapper.getKeyspace(), entityMapper.getTable(), gatherUDTFields(entityMapper));
    }

    UDTDefinition getUdtDefinition() {
        return udtDefinition;
    }

    UDTValue toUDTValue(T nestedEntity) {
        UDTValue udtValue = new UDTValue(udtDefinition);
        for (ColumnMapper<T> cm : entityMapper.allColumns()) {
            Object value = cm.getValue(nestedEntity);
            udtValue.setBytesUnsafe(cm.getColumnName(), value == null ? null : cm.getDataType().serialize(value, UDT_PROTOCOL_VERSION));
        }
        return udtValue;
    }
    
    List<UDTValue> toUDTValues(List<T> nestedEntities) {
        List<UDTValue> udtValues = new ArrayList<UDTValue>(nestedEntities.size());
        for (T nestedEntity : nestedEntities) {
            UDTValue udtValue = toUDTValue(nestedEntity);
            udtValues.add(udtValue);
        }            
        return udtValues;
    }
    
    Set<UDTValue> toUDTValues(Set<T> nestedEntities) {
        Set<UDTValue> udtValues = Sets.newHashSetWithExpectedSize(nestedEntities.size());
        for (T nestedEntity : nestedEntities) {
            UDTValue udtValue = toUDTValue(nestedEntity);
            udtValues.add(udtValue);
        }            
        return udtValues;
    }
    
    /*
     * Map conversion methods are static because they use two mappers, either of
     * which (but not both) may be null.
     * 
     * This reflects the fact that the map datatype can use UDTs as keys, or
     * values, or both.
     */
    static <K, V> Map<Object, Object> toUDTValues(Map<K, V> nestedEntities, NestedMapper<K> keyMapper, NestedMapper<V> valueMapper) {
        assert keyMapper != null || valueMapper != null;
        Map<Object, Object> udtValues = Maps.newHashMapWithExpectedSize(nestedEntities.size()); 
        for (Entry<K, V> entry : nestedEntities.entrySet()) {
            Object key = (keyMapper == null) ? entry.getKey() : keyMapper.toUDTValue(entry.getKey());
            Object value = (valueMapper == null) ? entry.getValue() : valueMapper.toUDTValue(entry.getValue());
            udtValues.put(key, value);
        }
        return udtValues;
    }

    T toNestedEntity(UDTValue udtValue) {
        T nestedEntity = entityMapper.newEntity();
        for (ColumnMapper<T> cm : entityMapper.allColumns()) {
            ByteBuffer bytes = udtValue.getBytesUnsafe(cm.getColumnName());
            if (bytes != null)
                cm.setValue(nestedEntity, cm.getDataType().deserialize(bytes, UDT_PROTOCOL_VERSION));
        }
        return nestedEntity;
    }
    
    List<T> toNestedEntities(List<UDTValue> udtValues) {
        List<T> nestedEntities = new ArrayList<T>(udtValues.size());
        for (UDTValue udtValue : udtValues) {
            nestedEntities.add(toNestedEntity(udtValue));
        }
        return nestedEntities;
    }
    
    Set<T> toNestedEntities(Set<UDTValue> udtValues) {
        Set<T> nestedEntities = Sets.newHashSetWithExpectedSize(udtValues.size());
        for (UDTValue udtValue : udtValues) {
            nestedEntities.add(toNestedEntity(udtValue));
        }
        return nestedEntities;
    }
    
    @SuppressWarnings("unchecked")
    static <K, V> Map<K, V> toNestedEntities(Map<Object, Object> udtValues, NestedMapper<K> keyMapper, NestedMapper<V> valueMapper) {
        Map<K, V> nestedEntities = Maps.newHashMapWithExpectedSize(udtValues.size());
        for (Entry<Object, Object> entry : udtValues.entrySet()) {
            K key = (keyMapper == null) ? (K) entry.getKey() : keyMapper.toNestedEntity((UDTValue) entry.getKey());
            V value = (valueMapper == null) ? (V) entry.getValue() : valueMapper.toNestedEntity((UDTValue)entry.getValue()); 
            nestedEntities.put(key, value);
        }
        return nestedEntities;
    }

    private static <T> Collection<Field> gatherUDTFields(EntityMapper<T> entityMapper) {
        List<ColumnMapper<T>> columns = entityMapper.allColumns();
        List<Field> fields = new ArrayList<Field>(columns.size());
        for (ColumnMapper<T> column : columns) {
            fields.add(new Field(column.getColumnName(), column.getDataType()));
        }
        return fields;
    }
}
