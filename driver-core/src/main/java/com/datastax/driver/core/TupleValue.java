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

import java.util.ArrayList;
import java.util.List;

/**
 * A value for a Tuple.
 */
public class TupleValue extends AbstractData<TupleValue> {

    private final List<DataType> types;

    /**
     * Builds a new value for a tuple.
     *
     * @param types the types of the tuple's elements.
     */
    public TupleValue(List<DataType> types) {
        // All things in a tuple are encoded with the protocol v3
        super(3, types.size());
        this.types = types;
    }

    protected DataType getType(int i) {
        return types.get(i);
    }

    @Override
    protected String getName(int i) {
        throw new UnsupportedOperationException("The fields of a tuple don't have names");
    }

    @Override
    protected int[] getAllIndexesOf(String name) {
        throw new UnsupportedOperationException("The fields of a tuple don't have names");
    }

    /**
     * The types of the fields of this tuple.
     *
     * @return the types of the fields of this tuple.
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

    /**
     * Builds a tuple value containing the provided elements.
     *
     * <p>
     * The CQL {@code DataType} of each element is inferred from its Java class,
     * using the following correspondence:
     * <table>
     *   <caption>Java class to DataType correspondence</caption>
     *   <tr><th>Java Class </th><th>DataType (CQL)</th></tr>
     *   <tr><td>ByteBuffer </td><td>BLOB</td></tr>
     *   <tr><td>Integer    </td><td>INT</td></tr>
     *   <tr><td>Long       </td><td>BIGINT</td></tr>
     *   <tr><td>Float      </td><td>FLOAT</td></tr>
     *   <tr><td>Double     </td><td>DOUBLE</td></tr>
     *   <tr><td>BigDecimal </td><td>DECIMAL</td></tr>
     *   <tr><td>BigInteger </td><td>VARINT</td></tr>
     *   <tr><td>String     </td><td>TEXT</td></tr>
     *   <tr><td>Boolean    </td><td>BOOLEAN</td></tr>
     *   <tr><td>InetAddress</td><td>INET</td></tr>
     *   <tr><td>Date       </td><td>TIMESTAMP</td></tr>
     *   <tr><td>UUID       </td><td>UUID</td></tr>
     *   <tr><td>List       </td><td>LIST</td></tr>
     *   <tr><td>Set        </td><td>SET</td></tr>
     *   <tr><td>Map        </td><td>MAP</td></tr>
     *   <tr><td>UDTValue   </td><td>UDT</td></tr>
     *   <tr><td>TupleValue </td><td>TUPLE</td></tr>
     * </table>
     * <p>
     * Note that this might not be adequate for Java types that map to multiple
     * CQL types (for example, String could also be mapped to ASCII or VARCHAR).
     * Also, collection element types are inferred from the first element (or
     * first key/value pair for Map); for empty collections, the BLOB type is
     * used (for example, an empty List is mapped to LIST&lt;BLOB&gt;).
     * <p>
     * For cases where this type mapping is not what you want, use {@link #TupleValue(List)}
     * to pass an explicit list of types, then set the values manually.
     *
     * @param elements the elements.
     * @return a tuple value containing {@code elements}.
     */
    public static TupleValue of(Object... elements) {
        List<DataType> types = new ArrayList<DataType>(elements.length);
        for (Object element : elements) {
            DataType type = TypeCodec.getDataTypeFor(element);
            types.add(type);
        }
        TupleValue v = new TupleValue(types);
        for (int i = 0; i < elements.length; i++) {
            v.setBytesUnsafe(i, types.get(i).serialize(elements[i], 3));
        }
        return v;
    }
}
