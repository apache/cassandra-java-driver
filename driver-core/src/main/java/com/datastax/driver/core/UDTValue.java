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

/**
 * A value for a User Defined Type.
 */
public class UDTValue extends AbstractData<UDTValue> {

    private final UDTDefinition definition;

    UDTValue(UDTDefinition definition) {
        // All things in a UDT are encoded with the protocol v3
        super(3, definition.size());
        this.definition = definition;
    }

    protected DataType getType(int i) {
        return definition.byIdx[i].getType();
    }

    protected String getName(int i) {
        return definition.byIdx[i].getName();
    }

    protected int[] getAllIndexesOf(String name) {
        int[] indexes = definition.byName.get(name);
        if (indexes == null)
            throw new IllegalArgumentException(name + " is not a field defined in this UDT");
        return indexes;
    }

    /**
     * The definition of the UDT this is a value of.
     *
     * @return the definition of the UDT this is a value of.
     */
    public UDTDefinition getDefinition() {
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
        // TODO: we should make the output CQL-compatible, i.e. we should
        // quote string etc... But to do properly we sould move some of the
        // formatting code from the queryBuilder to DataType (say some DataType.format(Object))
        // method.
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (int i = 0; i < values.length; i++) {
            if (i > 0)
                sb.append(",");

            sb.append(getName(i));
            sb.append(":");
            sb.append(values[i] == null ? "null" : getType(i).deserialize(values[i]));
        }
        sb.append("}");
        return sb.toString();
    }
}
