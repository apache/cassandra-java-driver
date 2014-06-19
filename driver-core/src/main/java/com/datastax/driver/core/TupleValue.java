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
 * A value for a User Defined Type.
 */
public class TupleValue extends AbstractData<TupleValue> {

    private final DataType[] types;

    public TupleValue(int protocolVersion, List<DataType> types) {
        this(protocolVersion, types.toArray(new DataType[types.size()]));
    }

    TupleValue(int protocolVersion, DataType[] types) {
        // All things in a UDT are encoded with the protocol v3
        super(protocolVersion, types.length);
        this.types = types;
    }

    @Override protected DataType getType(int i) {
        return types[i];
    }

    @Override protected String getName(int i) {
        return null;
    }

    @Override protected int[] getAllIndexesOf(String name) {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TupleValue))
            return false;

        TupleValue that = (TupleValue)o;
        if (!Arrays.equals(types, that.types))
            return false;

        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        // TODO: we should make the output CQL-compatible, i.e. we should
        // quote string etc... But to do properly we sould move some of the
        // formatting code from the queryBuilder to DataType (say some DataType.format(Object))
        // method.
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        for (int i = 0; i < values.length; i++) {
            if (i > 0)
                sb.append(',');
            sb.append(values[i] == null ? "null" : getType(i).deserialize(values[i], version));
        }
        sb.append('}');
        return sb.toString();
    }

    public int size() {
        return values.length;
    }
}
