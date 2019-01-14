/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.metadata.token;

import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import java.nio.ByteBuffer;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class Murmur3TokenFactory implements TokenFactory {

  public static final String PARTITIONER_NAME = "org.apache.cassandra.dht.Murmur3Partitioner";

  public static final Murmur3Token MIN_TOKEN = new Murmur3Token(Long.MIN_VALUE);
  public static final Murmur3Token MAX_TOKEN = new Murmur3Token(Long.MAX_VALUE);

  @Override
  public String getPartitionerName() {
    return PARTITIONER_NAME;
  }

  @Override
  public Token hash(ByteBuffer partitionKey) {
    long v = murmur(partitionKey);
    return new Murmur3Token(v == Long.MIN_VALUE ? Long.MAX_VALUE : v);
  }

  @Override
  public Token parse(String tokenString) {
    return new Murmur3Token(Long.parseLong(tokenString));
  }

  @Override
  public String format(Token token) {
    Preconditions.checkArgument(
        token instanceof Murmur3Token, "Can only format Murmur3Token instances");
    return Long.toString(((Murmur3Token) token).getValue());
  }

  @Override
  public Token minToken() {
    return MIN_TOKEN;
  }

  @Override
  public TokenRange range(Token start, Token end) {
    Preconditions.checkArgument(
        start instanceof Murmur3Token && end instanceof Murmur3Token,
        "Can only build ranges of Murmur3Token instances");
    return new Murmur3TokenRange((Murmur3Token) start, (Murmur3Token) end);
  }

  // This is an adapted version of the MurmurHash.hash3_x64_128 from Cassandra used
  // for M3P. Compared to that methods, there's a few inlining of arguments and we
  // only return the first 64-bits of the result since that's all M3P uses.
  private long murmur(ByteBuffer data) {
    int offset = data.position();
    int length = data.remaining();

    int nblocks = length >> 4; // Process as 128-bit blocks.

    long h1 = 0;
    long h2 = 0;

    long c1 = 0x87c37b91114253d5L;
    long c2 = 0x4cf5ad432745937fL;

    // ----------
    // body

    for (int i = 0; i < nblocks; i++) {
      long k1 = getblock(data, offset, i * 2);
      long k2 = getblock(data, offset, i * 2 + 1);

      k1 *= c1;
      k1 = rotl64(k1, 31);
      k1 *= c2;
      h1 ^= k1;
      h1 = rotl64(h1, 27);
      h1 += h2;
      h1 = h1 * 5 + 0x52dce729;
      k2 *= c2;
      k2 = rotl64(k2, 33);
      k2 *= c1;
      h2 ^= k2;
      h2 = rotl64(h2, 31);
      h2 += h1;
      h2 = h2 * 5 + 0x38495ab5;
    }

    // ----------
    // tail

    // Advance offset to the unprocessed tail of the data.
    offset += nblocks * 16;

    long k1 = 0;
    long k2 = 0;

    switch (length & 15) {
      case 15:
        k2 ^= ((long) data.get(offset + 14)) << 48;
        // fall through
      case 14:
        k2 ^= ((long) data.get(offset + 13)) << 40;
        // fall through
      case 13:
        k2 ^= ((long) data.get(offset + 12)) << 32;
        // fall through
      case 12:
        k2 ^= ((long) data.get(offset + 11)) << 24;
        // fall through
      case 11:
        k2 ^= ((long) data.get(offset + 10)) << 16;
        // fall through
      case 10:
        k2 ^= ((long) data.get(offset + 9)) << 8;
        // fall through
      case 9:
        k2 ^= ((long) data.get(offset + 8));
        k2 *= c2;
        k2 = rotl64(k2, 33);
        k2 *= c1;
        h2 ^= k2;
        // fall through
      case 8:
        k1 ^= ((long) data.get(offset + 7)) << 56;
        // fall through
      case 7:
        k1 ^= ((long) data.get(offset + 6)) << 48;
        // fall through
      case 6:
        k1 ^= ((long) data.get(offset + 5)) << 40;
        // fall through
      case 5:
        k1 ^= ((long) data.get(offset + 4)) << 32;
        // fall through
      case 4:
        k1 ^= ((long) data.get(offset + 3)) << 24;
        // fall through
      case 3:
        k1 ^= ((long) data.get(offset + 2)) << 16;
        // fall through
      case 2:
        k1 ^= ((long) data.get(offset + 1)) << 8;
        // fall through
      case 1:
        k1 ^= ((long) data.get(offset));
        k1 *= c1;
        k1 = rotl64(k1, 31);
        k1 *= c2;
        h1 ^= k1;
    }

    // ----------
    // finalization

    h1 ^= length;
    h2 ^= length;

    h1 += h2;
    h2 += h1;

    h1 = fmix(h1);
    h2 = fmix(h2);

    h1 += h2;

    return h1;
  }

  private long getblock(ByteBuffer key, int offset, int index) {
    int i_8 = index << 3;
    int blockOffset = offset + i_8;
    return ((long) key.get(blockOffset) & 0xff)
        + (((long) key.get(blockOffset + 1) & 0xff) << 8)
        + (((long) key.get(blockOffset + 2) & 0xff) << 16)
        + (((long) key.get(blockOffset + 3) & 0xff) << 24)
        + (((long) key.get(blockOffset + 4) & 0xff) << 32)
        + (((long) key.get(blockOffset + 5) & 0xff) << 40)
        + (((long) key.get(blockOffset + 6) & 0xff) << 48)
        + (((long) key.get(blockOffset + 7) & 0xff) << 56);
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
}
