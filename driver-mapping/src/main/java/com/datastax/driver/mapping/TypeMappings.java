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

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.mapping.annotations.UDT;

/**
 * Utility methods to determine which CQL type we expect for a given Java field type.
 */
class TypeMappings {

    static DataType getSimpleType(Class<?> klass, String fieldName) {
        if (ByteBuffer.class.isAssignableFrom(klass))
            return DataType.blob();

        if (klass == int.class || Integer.class.isAssignableFrom(klass))
                return DataType.cint();
        if (klass == long.class || Long.class.isAssignableFrom(klass))
            return DataType.bigint();
        if (klass == float.class || Float.class.isAssignableFrom(klass))
            return DataType.cfloat();
        if (klass == double.class || Double.class.isAssignableFrom(klass))
            return DataType.cdouble();
        if (klass == boolean.class || Boolean.class.isAssignableFrom(klass))
            return DataType.cboolean();

        if (BigDecimal.class.isAssignableFrom(klass))
            return DataType.decimal();
        if (BigInteger.class.isAssignableFrom(klass))
            return DataType.varint();

        if (String.class.isAssignableFrom(klass))
            return DataType.text();
        if (InetAddress.class.isAssignableFrom(klass))
            return DataType.inet();
        if (Date.class.isAssignableFrom(klass))
            return DataType.timestamp();
        if (UUID.class.isAssignableFrom(klass))
            return DataType.uuid();

        if (Collection.class.isAssignableFrom(klass))
            throw new IllegalArgumentException(String.format("Cannot map non-parametrized collection type %s for field %s; Please use a concrete type parameter", klass.getName(), fieldName));

        throw new IllegalArgumentException(String.format("Cannot map unknown class %s for field %s", klass.getName(), fieldName));
    }

    static boolean mapsToList(Class<?> klass) {
        return List.class.equals(klass);
    }

    static boolean mapsToSet(Class<?> klass) {
        return Set.class.equals(klass);
    }

    static boolean mapsToMap(Class<?> klass) {
        return Map.class.equals(klass);
    }

    static boolean mapsToEnum(Class<?> klass) { return klass.isEnum(); }

    static boolean mapsToCollection(Class<?> klass) {
        return mapsToList(klass) || mapsToSet(klass) || mapsToMap(klass);
    }

    static boolean isMappedUDT(Class<?> klass) {
        return klass.isAnnotationPresent(UDT.class);
    }

    static boolean mapsToUserTypeOrTuple(Class<?> klass) {
        return isMappedUDT(klass) ||
            klass.equals(UDTValue.class) ||
            klass.equals(TupleValue.class);
    }
}
