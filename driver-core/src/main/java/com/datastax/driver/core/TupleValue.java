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
package com.datastax.driver.core;

/**
 * A value for a Tuple.
 */
public class TupleValue extends AbstractAddressableByIndexData<TupleValue> {

    private final TupleType type;

    /**
     * Builds a new value for a tuple.
     *
     * @param type the {@link TupleType} instance defining this tuple's components.
     */
    TupleValue(TupleType type) {
        super(type.getProtocolVersion(), type.getComponentTypes().size());
        this.type = type;
    }

    protected DataType getType(int i) {
        return type.getComponentTypes().get(i);
    }

    @Override
    protected String getName(int i) {
        // This is used for error messages
        return "component " + i;
    }

    @Override
    protected CodecRegistry getCodecRegistry() {
        return type.getCodecRegistry();
    }

    /**
     * The tuple type this is a value of.
     *
     * @return The tuple type this is a value of.
     */
    public TupleType getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TupleValue))
            return false;

        TupleValue that = (TupleValue)o;
        if (!type.equals(that.type))
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

            if(values[i] == null)
                sb.append("null");
            else {
                DataType dt = getType(i);
                TypeCodec<Object> codec = getCodecRegistry().codecFor(dt);
                sb.append(codec.format(codec.deserialize(values[i], protocolVersion)));
            }
        }
        sb.append(")");
        return sb.toString();
    }
}
