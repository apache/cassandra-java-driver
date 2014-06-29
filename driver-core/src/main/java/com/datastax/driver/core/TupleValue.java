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

import java.util.Arrays;
import java.util.List;

/**
 * A value for a Tuple.
 */
public class TupleValue extends AbstractAddressableByIndexData<TupleValue> {

    private final List<DataType> types;

    /**
     * Builds a new value for a tuple.
     *
     * @param types the types of the tuple's components.
     */
    public TupleValue(List<DataType> types) {
        // All things in a tuple are encoded with the protocol v3
        super(3, types.size());
        this.types = types;
    }

    /**
     * Builds a new value for a tuple.
     *
     * @param types the types of the tuple's components.
     */
    public TupleValue(DataType... types) {
        this(Arrays.asList(types));
    }

    protected DataType getType(int i) {
        return types.get(i);
    }

    @Override
    protected String getName(int i) {
        // This is used for error messages
        return "component " + i;
    }

    /**
     * The types of the components of this tuple.
     *
     * @return the types of the components of this tuple.
     */
    public List<DataType> getTypes() {
        return types;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TupleValue))
            return false;

        TupleValue that = (TupleValue)o;
        if (!types.equals(that.types))
            return false;

        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        for (int i = 0; i < values.length; i++) {
            if (i > 0)
                sb.append(", ");

            DataType dt = getType(i);
            sb.append(values[i] == null ? "null" : dt.format(dt.deserialize(values[i], 3)));
        }
        sb.append(")");
        return sb.toString();
    }
}
