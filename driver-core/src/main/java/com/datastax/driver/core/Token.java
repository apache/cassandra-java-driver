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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/*
 * It's not an interface because we don't want to expose it
 * Note: we may want to expose this later if people use custom partitioner and want to be able to extend that.
 * This is way premature however.
 */
abstract class Token implements Comparable<Token> {

    public static Token.Factory getFactory(String partitionerName) {
        if (partitionerName.endsWith("Murmur3Partitioner"))
            return M3PToken.FACTORY;
        else if (partitionerName.endsWith("RandomPartitioner"))
            return RPToken.FACTORY;
        else if (partitionerName.endsWith("OrderedPartitioner"))
            return OPPToken.FACTORY;
        else
            return null;
    }

    public interface Factory {
        public Token fromString(String tokenStr);
        public Token hash(ByteBuffer partitionKey);
    }

    // Murmur3Partitioner tokens
    static class M3PToken extends Token {
        private final long value;

        public static final Factory FACTORY = new Factory() {

            private long getblock(ByteBuffer key, int offset, int index) {
                int i_8 = index << 3;
                int blockOffset = offset + i_8;
                return ((long) key.get(blockOffset + 0) & 0xff) + (((long) key.get(blockOffset + 1) & 0xff) << 8) +
                       (((long) key.get(blockOffset + 2) & 0xff) << 16) + (((long) key.get(blockOffset + 3) & 0xff) << 24) +
                       (((long) key.get(blockOffset + 4) & 0xff) << 32) + (((long) key.get(blockOffset + 5) & 0xff) << 40) +
                       (((long) key.get(blockOffset + 6) & 0xff) << 48) + (((long) key.get(blockOffset + 7) & 0xff) << 56);
            }

            private long rotl64(long v, int n) {
                return ((v << n) | (v >>> (64 - n)));
            }

            private long fmix(long k) {
                k ^= k >>> 33;
                k *= 0xff51afd7ed558ccdL;
                k ^= k >>> 33;
                k *= 0xc4ceb9fe1a85ec53L;
                k ^= k >>> 33;
                return k;
            }

            // This is an adapted version of the MurmurHash.hash3_x64_128 from Cassandra used
            // for M3P. Compared to that methods, there's a few inlining of arguments and we
            // only return the first 64-bits of the result since that's all M3P uses.
            @SuppressWarnings("fallthrough")
            private long murmur(ByteBuffer data) {
                int offset = data.position();
                int length = data.remaining();

                int nblocks = length >> 4; // Process as 128-bit blocks.

                long h1 = 0;
                long h2 = 0;

                long c1 = 0x87c37b91114253d5L;
                long c2 = 0x4cf5ad432745937fL;

                //----------
                // body

                for(int i = 0; i < nblocks; i++) {
                    long k1 = getblock(data, offset, i*2+0);
                    long k2 = getblock(data, offset, i*2+1);

                    k1 *= c1; k1 = rotl64(k1,31); k1 *= c2; h1 ^= k1;
                    h1 = rotl64(h1,27); h1 += h2; h1 = h1*5+0x52dce729;
                    k2 *= c2; k2  = rotl64(k2,33); k2 *= c1; h2 ^= k2;
                    h2 = rotl64(h2,31); h2 += h1; h2 = h2*5+0x38495ab5;
                }

                //----------
                // tail

                // Advance offset to the unprocessed tail of the data.
                offset += nblocks * 16;

                long k1 = 0;
                long k2 = 0;

                switch(length & 15) {
                    case 15: k2 ^= ((long) data.get(offset+14)) << 48;
                    case 14: k2 ^= ((long) data.get(offset+13)) << 40;
                    case 13: k2 ^= ((long) data.get(offset+12)) << 32;
                    case 12: k2 ^= ((long) data.get(offset+11)) << 24;
                    case 11: k2 ^= ((long) data.get(offset+10)) << 16;
                    case 10: k2 ^= ((long) data.get(offset+9)) << 8;
                    case  9: k2 ^= ((long) data.get(offset+8)) << 0;
                             k2 *= c2; k2  = rotl64(k2,33); k2 *= c1; h2 ^= k2;

                    case  8: k1 ^= ((long) data.get(offset+7)) << 56;
                    case  7: k1 ^= ((long) data.get(offset+6)) << 48;
                    case  6: k1 ^= ((long) data.get(offset+5)) << 40;
                    case  5: k1 ^= ((long) data.get(offset+4)) << 32;
                    case  4: k1 ^= ((long) data.get(offset+3)) << 24;
                    case  3: k1 ^= ((long) data.get(offset+2)) << 16;
                    case  2: k1 ^= ((long) data.get(offset+1)) << 8;
                    case  1: k1 ^= ((long) data.get(offset));
                             k1 *= c1; k1  = rotl64(k1,31); k1 *= c2; h1 ^= k1;
                };

                //----------
                // finalization

                h1 ^= length; h2 ^= length;

                h1 += h2;
                h2 += h1;

                h1 = fmix(h1);
                h2 = fmix(h2);

                h1 += h2;
                h2 += h1;

                return h1;
            }

            @Override
            public M3PToken fromString(String tokenStr) {
                return new M3PToken(Long.parseLong(tokenStr));
            }

            @Override
            public M3PToken hash(ByteBuffer partitionKey) {
                long v = murmur(partitionKey);
                return new M3PToken(v == Long.MIN_VALUE ? Long.MAX_VALUE : v);
            }
        };

        private M3PToken(long value) {
            this.value = value;
        }

        @Override
        public int compareTo(Token other) {
            assert other instanceof M3PToken;
            long otherValue = ((M3PToken)other).value;
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
    static class OPPToken extends Token {
        private final ByteBuffer value;

        public static final Factory FACTORY = new Factory() {
            @Override
            public OPPToken fromString(String tokenStr) {
                return new OPPToken(TypeCodec.StringCodec.utf8Instance.serialize(tokenStr));
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
        public int compareTo(Token other) {
            assert other instanceof OPPToken;
            return value.compareTo(((OPPToken)other).value);
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
    static class RPToken extends Token {

        private final BigInteger value;

        public static final Factory FACTORY = new Factory() {

            private BigInteger md5(ByteBuffer data) {
                try {
                    MessageDigest digest = MessageDigest.getInstance("MD5");
                    digest.update(data.duplicate());
                    return new BigInteger(digest.digest()).abs();
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException("MD5 doesn't seem to be available on this JVM", e);
                }
            }

            @Override
            public RPToken fromString(String tokenStr) {
                return new RPToken(new BigInteger(tokenStr));
            }

            @Override
            public RPToken hash(ByteBuffer partitionKey) {
                return new RPToken(md5(partitionKey));
            }
        };

        private RPToken(BigInteger value) {
            this.value = value;
        }

        @Override
        public int compareTo(Token other) {
            assert other instanceof RPToken;
            return value.compareTo(((RPToken)other).value);
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
