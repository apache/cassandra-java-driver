/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import net.jcip.annotations.Immutable;

@Immutable
public class ByteOrderedTokenRange extends TokenRangeBase {

  private static final BigInteger TWO = BigInteger.valueOf(2);

  public ByteOrderedTokenRange(ByteOrderedToken start, ByteOrderedToken end) {
    super(start, end, ByteOrderedTokenFactory.MIN_TOKEN);
  }

  @Override
  protected TokenRange newTokenRange(Token start, Token end) {
    return new ByteOrderedTokenRange(((ByteOrderedToken) start), ((ByteOrderedToken) end));
  }

  @Override
  protected List<Token> split(Token rawStartToken, Token rawEndToken, int numberOfSplits) {
    int tokenOrder = rawStartToken.compareTo(rawEndToken);

    // ]min,min] means the whole ring. However, since there is no "max token" with this partitioner,
    // we can't come up with a magic end value that would cover the whole ring
    if (tokenOrder == 0 && rawStartToken.equals(ByteOrderedTokenFactory.MIN_TOKEN)) {
      throw new IllegalArgumentException("Cannot split whole ring with ordered partitioner");
    }

    ByteOrderedToken startToken = (ByteOrderedToken) rawStartToken;
    ByteOrderedToken endToken = (ByteOrderedToken) rawEndToken;

    int significantBytes;
    BigInteger start, end, range, ringEnd, ringLength;
    BigInteger bigNumberOfSplits = BigInteger.valueOf(numberOfSplits);
    if (tokenOrder < 0) {
      // Since tokens are compared lexicographically, convert to integers using the largest length
      // (ex: given 0x0A and 0x0BCD, switch to 0x0A00 and 0x0BCD)
      significantBytes = Math.max(startToken.getValue().capacity(), endToken.getValue().capacity());

      // If the number of splits does not fit in the difference between the two integers, use more
      // bytes (ex: cannot fit 4 splits between 0x01 and 0x03, so switch to 0x0100 and 0x0300)
      // At most 4 additional bytes will be needed, since numberOfSplits is an integer.
      int addedBytes = 0;
      while (true) {
        start = toBigInteger(startToken.getValue(), significantBytes);
        end = toBigInteger(endToken.getValue(), significantBytes);
        range = end.subtract(start);
        if (addedBytes == 4 || start.equals(end) || range.compareTo(bigNumberOfSplits) >= 0) {
          break;
        }
        significantBytes += 1;
        addedBytes += 1;
      }
      ringEnd = ringLength = null; // won't be used
    } else {
      // Same logic except that we wrap around the ring
      significantBytes = Math.max(startToken.getValue().capacity(), endToken.getValue().capacity());
      int addedBytes = 0;
      while (true) {
        start = toBigInteger(startToken.getValue(), significantBytes);
        end = toBigInteger(endToken.getValue(), significantBytes);
        ringLength = TWO.pow(significantBytes * 8);
        ringEnd = ringLength.subtract(BigInteger.ONE);
        range = end.subtract(start).add(ringLength);
        if (addedBytes == 4 || range.compareTo(bigNumberOfSplits) >= 0) {
          break;
        }
        significantBytes += 1;
        addedBytes += 1;
      }
    }

    List<BigInteger> values = super.split(start, range, ringEnd, ringLength, numberOfSplits);
    List<Token> tokens = Lists.newArrayListWithExpectedSize(values.size());
    for (BigInteger value : values) {
      tokens.add(new ByteOrderedToken(toBytes(value, significantBytes)));
    }
    return tokens;
  }

  // Convert a token's byte array to a number in order to perform computations.
  // This depends on the number of "significant bytes" that we use to normalize all tokens to the
  // same size.
  // For example if the token is 0x01 but significantBytes is 2, the result is 8 (0x0100).
  private BigInteger toBigInteger(ByteBuffer bb, int significantBytes) {
    byte[] bytes = Bytes.getArray(bb);
    byte[] target;
    if (significantBytes != bytes.length) {
      target = new byte[significantBytes];
      System.arraycopy(bytes, 0, target, 0, bytes.length);
    } else {
      target = bytes;
    }
    return new BigInteger(1, target);
  }

  // Convert a numeric representation back to a byte array.
  // Again, the number of significant bytes matters: if the input value is 1 but significantBytes is
  // 2, the
  // expected result is 0x0001 (a simple conversion would produce 0x01).
  protected ByteBuffer toBytes(BigInteger value, int significantBytes) {
    byte[] rawBytes = value.toByteArray();
    byte[] result;
    if (rawBytes.length == significantBytes) {
      result = rawBytes;
    } else {
      result = new byte[significantBytes];
      int start, length;
      if (rawBytes[0] == 0) {
        // that's a sign byte, ignore (it can cause rawBytes.length == significantBytes + 1)
        start = 1;
        length = rawBytes.length - 1;
      } else {
        start = 0;
        length = rawBytes.length;
      }
      System.arraycopy(rawBytes, start, result, significantBytes - length, length);
    }
    return ByteBuffer.wrap(result);
  }
}
