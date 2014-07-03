/*
 *      Copyright (C) 2012 DataStax Inc.
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
package com.datastax.driver.core;

import java.util.*;

import com.google.common.collect.ImmutableList;

/**
 * A tuple type.
 * <p>
 * A tuple type is a essentially a list of types.
 */
public class TupleType extends DataType {

    private final List<DataType> types;

    TupleType(List<DataType> types) {
        super(DataType.Name.TUPLE);
        this.types = ImmutableList.copyOf(types);
    }

    @SuppressWarnings("unchecked")
    @Override
    TypeCodec<Object> codec(int protocolVersion) {
        return (TypeCodec)TypeCodec.tupleOf(this);
    }

    /**
     * Creates a tuple type given a list of types.
     *
     * @param types the types for the tuple type.
     * @return the newly created tuple type.
     */
    public static TupleType of(DataType... types) {
        return new TupleType(Arrays.asList(types));
    }

    /**
     * The (immutable) list of types composing this tuple type.
     *
     * @return the (immutable) list of types composing this tuple type.
     */
    public List<DataType> getComponentTypes() {
        return types;
    }

    /**
     * Returns a new empty value for this tuple type.
     *
     * @return an empty (with all component to {@code null}) value for this
     * user type definition.
     */
    public TupleValue newValue() {
        return new TupleValue(this);
    }

    /**
     * Returns a new value for this tuple type that uses the provided values
     * for the components.
     * <p>
     * The numbers of values passed to this method must correspond to the
     * number of components in this tuple type. The {@code i}th parameter
     * value will then be assigned to the {@code i}th component of the resulting
     * tuple value.
     *
     * @param values the values to use for the component of the resulting
     * tuple.
     * @return a new tuple values based on the provided values.
     *
     * @throws IllegalArgumentException if the number of {@code values}
     * provided does not correspond to the number of components in this tuple
     * type.
     * @throws InvalidTypeException if any of the provided value is not of
     * the correct type for the component.
     */
    public TupleValue newValue(Object... values) {
        if (values.length != types.size())
            throw new IllegalArgumentException(String.format("Invalid number of values. Expecting %d but got %d", types.size(), values.length));

        TupleValue t = newValue();
        for (int i = 0; i < values.length; i++)
            t.setValue(i, values[i] == null ? null : types.get(i).serialize(values[i], 3));
        return t;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(new Object[]{ name, types });
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TupleType))
            return false;

        TupleType d = (TupleType)o;
        return name == d.name && types.equals(d.types);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (DataType type : types) {
            sb.append(sb.length() == 0 ? "tuple<" : ", ");
            sb.append(type);
        }
        return sb.append(">").toString();
    }
}
