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

import com.datastax.driver.core.DataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CollectionEnumMapper<T> {
    private Map<String, Object> fromString;
    private Class<T> type;

    public CollectionEnumMapper(Class<T> type) {
        this.type = type;
        this.fromString = new HashMap<String, Object>(type.getEnumConstants().length);
        for (Object constant : type.getEnumConstants())
            fromString.put(constant.toString().toLowerCase(), constant);
    }

    T fromCql(Object value, EnumType enumType) {
        Object converted = null;
        switch (enumType) {
            case STRING:
                converted = fromString.get(value.toString().toLowerCase());
                break;
            case ORDINAL:
                converted = type.getEnumConstants()[(Integer)value];
                break;
        }
        return (T) converted;
    }

    Object toCql(T value, EnumType enumType) {
        switch (enumType) {
            case STRING:
                return (value == null) ? null : value.toString();
            case ORDINAL:
                return (value == null) ? null : ((Enum)value).ordinal();
        }
        throw new AssertionError();
    }

    /**
     * Handles a (possibly nested) collection where some of the elements are CQL Int or Strings and
     * should be converted to enums
     */
    @SuppressWarnings("unchecked")
    static Object convertToEnum(Object value, InferredCQLType type, EnumType enumType) {
        if (value == null)
            return null;

        if (!type.containsEnum)
            return value;

        if (type.enumMapper != null)
            return type.enumMapper.fromCql(value, enumType);

        if (type.dataType.getName() == DataType.Name.LIST) {
            InferredCQLType elementType = type.childTypes.get(0);
            List<Object> result = new ArrayList<Object>();
            for (Object element : (List<Object>)value)
                result.add(convertToEnum(element, elementType, enumType));
            return result;
        }

        if (type.dataType.getName() == DataType.Name.SET) {
            InferredCQLType elementType = type.childTypes.get(0);
            Set<Object> result = new LinkedHashSet<Object>();
            for (Object element : (Set<Object>)value)
                result.add(convertToEnum(element, elementType, enumType));
            return result;
        }

        if (type.dataType.getName() == DataType.Name.MAP) {
            InferredCQLType keyType = type.childTypes.get(0);
            InferredCQLType valueType = type.childTypes.get(1);
            Map<Object, Object> result = new LinkedHashMap<Object, Object>();
            for (Map.Entry<Object, Object> entry : ((Map<Object, Object>)value).entrySet())
                result.put(
                        convertToEnum(entry.getKey(), keyType, enumType),
                        convertToEnum(entry.getValue(), valueType, enumType)
                );
            return result;
        }
        throw new IllegalArgumentException("Error converting " + value);
    }

    /**
     * Handles a (possibly nested) collection where some of the elements are {@code Enum}
     * instances that must be converted
     */
    @SuppressWarnings("unchecked")
    static Object convertToCql(Object value, InferredCQLType type, EnumType enumType) {
        if (value == null)
            return null;

        if (!type.containsEnum)
            return value;

        if (type.enumMapper != null)
            return type.enumMapper.toCql(value, enumType);

        if (type.dataType.getName() == DataType.Name.LIST) {
            InferredCQLType elementType = type.childTypes.get(0);
            List<Object> result = new ArrayList<Object>();
            for (Object element : (List<Object>)value)
                result.add(convertToCql(element, elementType, enumType));
            return result;
        }

        if (type.dataType.getName() == DataType.Name.SET) {
            InferredCQLType elementType = type.childTypes.get(0);
            Set<Object> result = new LinkedHashSet<Object>();
            for (Object element : (Set<Object>)value)
                result.add(convertToCql(element, elementType, enumType));
            return result;
        }

        if (type.dataType.getName() == DataType.Name.MAP) {
            InferredCQLType keyType = type.childTypes.get(0);
            InferredCQLType valueType = type.childTypes.get(1);
            Map<Object, Object> result = new LinkedHashMap<Object, Object>();
            for (Map.Entry<Object, Object> entry : ((Map<Object, Object>)value).entrySet())
                result.put(
                        convertToCql(entry.getKey(), keyType, enumType),
                        convertToCql(entry.getValue(), valueType, enumType)
                );
            return result;
        }
        throw new IllegalArgumentException("Error converting " + value);
    }
}
