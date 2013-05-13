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

import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MurmurHash;

// We really only use the generic for type safety and it's not an interface because we don't want to expose it
// Note: we may want to expose this later if people use custom partitioner and want to be able to extend that.
// This is way premature however.
abstract class Token<T extends Token<T>> implements Comparable<T> {

    public static Token.Factory<?> getFactory(String partitionerName) {
        if (partitionerName.endsWith("Murmur3Partitioner"))
            return M3PToken.FACTORY;
        else if (partitionerName.endsWith("RandomPartitioner"))
            return RPToken.FACTORY;
        else if (partitionerName.endsWith("OrderedPartitioner"))
            return OPPToken.FACTORY;
        else
            return null;
    }

    public interface Factory<T extends Token<T>> {
        public T fromString(String tokenStr);
        public T hash(ByteBuffer partitionKey);
    }

    // Murmur3Partitioner tokens
    static class M3PToken extends Token<M3PToken> {
        private final long value;

        public static final Factory<M3PToken> FACTORY = new Factory<M3PToken>() {
            @Override
            public M3PToken fromString(String tokenStr) {
                return new M3PToken(Long.parseLong(tokenStr));
            }

            @Override
            public M3PToken hash(ByteBuffer partitionKey) {
                long v = MurmurHash.hash3_x64_128(partitionKey, partitionKey.position(), partitionKey.remaining(), 0)[0];
                return new M3PToken(v == Long.MIN_VALUE ? Long.MAX_VALUE : v);
            }
        };

        private M3PToken(long value) {
            this.value = value;
        }

        @Override
        public int compareTo(M3PToken other) {
            long otherValue = other.value;
            return value < otherValue ? -1 : (value == otherValue) ? 0 : 1;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null || this.getClass() != obj.getClass())
                return false;

            return value == ((M3PToken)obj).value;
        }

        @Override
        public int hashCode() {
            return (int)(value^(value>>>32));
        }
    }

    // OPPartitioner tokens
    static class OPPToken extends Token<OPPToken> {
        private final ByteBuffer value;

        public static final Factory<OPPToken> FACTORY = new Factory<OPPToken>() {
            @Override
            public OPPToken fromString(String tokenStr) {
                return new OPPToken(ByteBufferUtil.bytes(tokenStr));
            }

            @Override
            public OPPToken hash(ByteBuffer partitionKey) {
                return new OPPToken(partitionKey);
            }
        };

        private OPPToken(ByteBuffer value) {
            this.value = value;
        }

        @Override
        public int compareTo(OPPToken other) {
            return value.compareTo(other.value);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null || this.getClass() != obj.getClass())
                return false;

            return value.equals(((OPPToken)obj).value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }

    // RandomPartitioner tokens
    static class RPToken extends Token<RPToken> {
        private final BigInteger value;

        public static final Factory<RPToken> FACTORY = new Factory<RPToken>() {
            @Override
            public RPToken fromString(String tokenStr) {
                return new RPToken(new BigInteger(tokenStr));
            }

            @Override
            public RPToken hash(ByteBuffer partitionKey) {
                return new RPToken(FBUtilities.hashToBigInteger(partitionKey));
            }
        };

        private RPToken(BigInteger value) {
            this.value = value;
        }

        @Override
        public int compareTo(RPToken other) {
            return value.compareTo(other.value);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null || this.getClass() != obj.getClass())
                return false;

            return value.equals(((RPToken)obj).value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }
}
