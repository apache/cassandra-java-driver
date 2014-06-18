package com.datastax.driver.mapping;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datastax.driver.core.*;
import com.datastax.driver.core.UDTDefinition.Field;

/**
 * An object handling the mapping of a particular class to a UDT.
 * <p>
 * A {@code UDTMapper} object is obtained from a {@code MappingManager} using the
 * {@link MappingManager#udtMapper} method.
 * </p>
 */
public class UDTMapper<T> {
    // UDTs are always serialized with the v3 protocol
    private static final int UDT_PROTOCOL_VERSION = Math.max(ProtocolOptions.NEWEST_SUPPORTED_PROTOCOL_VERSION, 3);

    private final EntityMapper<T> entityMapper;
    private final UDTDefinition udtDefinition;

    UDTMapper(EntityMapper<T> entityMapper) {
        this.entityMapper = entityMapper;
        this.udtDefinition = new UDTDefinition(entityMapper.getKeyspace(), entityMapper.getTable(), gatherUDTFields(entityMapper));
    }

    /**
     * Converts a {@link UDTValue} (returned by the core API) to the mapped
     * class.
     *
     * @param v the {@code UDTValue}.
     * @return an instance of the mapped class.
     *
     * @throws IllegalArgumentException if the {@code UDTValue} is not of the
     * type indicated in the mapped class's {@code @UDT} annotation.
     */
    public T map(UDTValue v) {
        if (!v.getDefinition().equals(udtDefinition)) {
            String message = String.format("UDT conversion mismatch: expected type %s.%s, got %s.%s",
                                           udtDefinition.getKeyspace(),
                                           udtDefinition.getName(),
                                           v.getDefinition().getKeyspace(),
                                           v.getDefinition().getName());
            throw new IllegalArgumentException(message);
        }
        return toNestedEntity(v);
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
    static <K, V> Map<Object, Object> toUDTValues(Map<K, V> nestedEntities, UDTMapper<K> keyMapper, UDTMapper<V> valueMapper) {
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
    static <K, V> Map<K, V> toNestedEntities(Map<Object, Object> udtValues, UDTMapper<K> keyMapper, UDTMapper<V> valueMapper) {
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
