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

import com.google.common.base.Joiner;

/**
 * A value for a User Defined Type.
 */
public class UDTValue extends AbstractData<UDTValue> {

    private final UserType definition;

    UDTValue(UserType definition) {
        super(definition.getProtocolVersion(), definition.size());
        this.definition = definition;
    }

    protected DataType getType(int i) {
        return definition.getFields()[i].getType();
    }

    protected String getName(int i) {
        return definition.getFields()[i].getName();
    }

    @Override
    protected CodecRegistry getCodecRegistry() {
        return definition.getCodecRegistry();
    }

    protected int[] getAllIndexesOf(String name) {
        int[] indexes = definition.getFieldIndicesByName().get(Metadata.handleId(name));
        if (indexes == null)
            throw new IllegalArgumentException(name + " is not a field defined in this UDT");
        return indexes;
    }

    /**
     * The UDT this is a value of.
     *
     * @return the UDT this is a value of.
     */
    public UserType getType() {
        return definition;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof UDTValue))
            return false;

        UDTValue that = (UDTValue)o;
        if (!definition.equals(that.definition))
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
        sb.append("{");
        for (int i = 0; i < values.length; i++) {
            if (i > 0)
                sb.append(",");

            sb.append(getName(i));
            sb.append(":");

            if(values[i] == null)
                sb.append("null");
            else {
                DataType dt = getType(i);
                TypeCodec<Object> codec = getCodecRegistry().codecFor(dt);
                sb.append(codec.format(codec.deserialize(values[i], protocolVersion)));
            }
        }
        sb.append("}");
        return sb.toString();
    }
}
