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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import org.testng.annotations.Test;

import static com.google.common.base.Preconditions.checkArgument;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * Demonstrates how to write custom codecs for high-performance deserialization of CQL collections to primitive arrays.
 */
public class TypeCodecArraysIntegrationTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList(
            "CREATE TABLE lists(i int PRIMARY KEY, l list<int>)",
            "INSERT INTO lists (i, l) VALUES (1, [1, 2, 3])"
        );
    }

    @Override
    protected Cluster.Builder configure(Cluster.Builder builder) {
        return builder.withCodecRegistry(
            new CodecRegistry().register(new IntArrayCodec())
        );
    }

    @Test(groups = "short")
    public void should_read_list_column_as_array() {
        Row row = session.execute("SELECT l FROM lists WHERE i = 1").one();

        int[] ints = row.get("l", int[].class);

        assertThat(ints).containsExactly(1, 2, 3);
    }

    @Test(groups = "short")
    public void should_set_list_column_with_array() {
        PreparedStatement pst = session.prepare("INSERT INTO lists (i, l) VALUES (?, ?)");
        BoundStatement bs = pst.bind()
            .setInt(0, 2)
            .set(1, new int[]{ 2, 3, 4 }, int[].class);
        session.execute(bs);

        Row row = session.execute("SELECT l FROM lists WHERE i = 2").one();
        assertThat(row.getList("l", Integer.class)).containsExactly(2, 3, 4);
    }

    @Test(groups = "short")
    public void should_format_array_as_cql_literal() {
        IntArrayCodec codec = new IntArrayCodec();

        assertThat(codec.format(new int[]{ 1, 2, 3 }))
            .isEqualTo("[1,2,3]");
        assertThat(codec.format(new int[0]))
            .isEqualTo("[]");
        assertThat(codec.format(null))
            .isEqualTo("null");
    }

    @Test(groups = "short")
    public void should_parse_array_from_cql_literal() {
        IntArrayCodec codec = new IntArrayCodec();

        assertThat(codec.parse("[1,2,3]"))
            .containsExactly(1, 2, 3);
        assertThat(codec.parse("null"))
            .isNull();
        assertThat(codec.parse(""))
            .isNull();
        assertThat(codec.parse(null))
            .isNull();
    }

    public static class IntArrayCodec extends TypeCodec<int[]> {

        private static final int SIZE_OF_INT = 4;

        public IntArrayCodec() {
            super(DataType.list(DataType.cint()), int[].class);
        }

        @Override
        public ByteBuffer serialize(int[] ints, ProtocolVersion protocolVersion) throws InvalidTypeException {
            if (ints == null)
                return null;

            boolean isProtocolV3OrAbove = protocolVersion.compareTo(ProtocolVersion.V2) > 0;

            checkArgument(isProtocolV3OrAbove || ints.length < 65536,
                "Native protocol version %d supports up to 65535 elements in any collection - but collection contains %d elements",
                protocolVersion.toInt(), ints.length);

            /*
             * Encoding of lists in the native protocol:
             * [size of list] [size of element 1][element1] [size of element 2][element2]...
             * Sizes are encoded on 2 bytes in protocol v1 and v2, 4 bytes otherwise.
             * Integers are always encoded on 4 bytes.
             * (See native_protocol_v*.spec in https://github.com/apache/cassandra/blob/trunk/doc/)
             */

            // Number of bytes to encode sizes
            int sizeOfSize = isProtocolV3OrAbove ? 4 : 2;

            // Number of bytes to encode each element (preceded by its size)
            int sizeOfElement = sizeOfSize + SIZE_OF_INT;

            // Number of bytes to encode the whole collection (size of collection + all elements)
            int totalSize = sizeOfSize + ints.length * sizeOfElement;

            ByteBuffer output = ByteBuffer.allocate(totalSize);

            writeSize(output, ints.length, protocolVersion);
            for (int i : ints) {
                writeSize(output, SIZE_OF_INT, protocolVersion);
                output.putInt(i);
            }
            output.flip();
            return output;
        }

        @Override
        public int[] deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
            if (bytes == null || bytes.remaining() == 0)
                return new int[0];

            boolean isProtocolV3OrAbove = protocolVersion.compareTo(ProtocolVersion.V2) > 0;
            int sizeOfSize = isProtocolV3OrAbove ? 4 : 2;

            ByteBuffer input = bytes.duplicate();

            int size = CodecUtils.readCollectionSize(input, protocolVersion);
            int[] ints = new int[size];
            for (int i = 0; i < size; i++) {
                // Skip size (we know it will always be 4 since the elements are ints)
                input.position(input.position() + sizeOfSize);
                ints[i] = input.getInt();
            }
            return ints;
        }

        @Override
        public String format(int[] ints) throws InvalidTypeException {
            if (ints == null)
                return "null";

            StringBuilder sb = new StringBuilder();
            sb.append('[');
            for (int i = 0; i < ints.length; i++) {
                if (i != 0)
                    sb.append(",");
                sb.append(ints[i]);
            }
            sb.append(']');
            return sb.toString();
        }

        @Override
        public int[] parse(String value) throws InvalidTypeException {
            if (value == null || value.isEmpty() || value.equals("null"))
                return null;

            int idx = ParseUtils.skipSpaces(value, 0);
            if (value.charAt(idx++) != '[')
                throw new InvalidTypeException(String.format("cannot parse list value from \"%s\", at character %d expecting '[' but got '%c'", value, idx, value.charAt(idx)));

            idx = ParseUtils.skipSpaces(value, idx);

            if (value.charAt(idx) == ']')
                return new int[0];

            List<Integer> l = Lists.newArrayList();
            while (idx < value.length()) {
                int n;
                try {
                    n = ParseUtils.skipCQLValue(value, idx);
                } catch (IllegalArgumentException e) {
                    throw new InvalidTypeException(String.format("Cannot parse list value from \"%s\", invalid CQL value at character %d", value, idx), e);
                }

                l.add(Integer.parseInt(value.substring(idx, n)));
                idx = n;

                idx = ParseUtils.skipSpaces(value, idx);
                if (value.charAt(idx) == ']')
                    return Ints.toArray(l);
                if (value.charAt(idx++) != ',')
                    throw new InvalidTypeException(String.format("Cannot parse list value from \"%s\", at character %d expecting ',' but got '%c'", value, idx, value.charAt(idx)));

                idx = ParseUtils.skipSpaces(value, idx);
            }
            throw new InvalidTypeException(String.format("Malformed list value \"%s\", missing closing ']'", value));
        }

        private void writeSize(ByteBuffer output, int size, ProtocolVersion protocolVersion) {
            switch (protocolVersion) {
                case V1:
                case V2:
                    output.putShort((short)size);
                    break;
                case V3:
                case V4:
                    output.putInt(size);
                    break;
                default:
                    throw protocolVersion.unsupported();
            }
        }
    }
}