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
import java.math.BigInteger;
import java.util.List;
import net.jcip.annotations.Immutable;

@Immutable
public class Murmur3TokenRange extends TokenRangeBase {

  private static final BigInteger RING_END = BigInteger.valueOf(Long.MAX_VALUE);
  private static final BigInteger RING_LENGTH =
      RING_END.subtract(BigInteger.valueOf(Long.MIN_VALUE));

  public Murmur3TokenRange(Murmur3Token start, Murmur3Token end) {
    super(start, end, Murmur3TokenFactory.MIN_TOKEN);
  }

  @Override
  protected TokenRange newTokenRange(Token start, Token end) {
    return new Murmur3TokenRange((Murmur3Token) start, (Murmur3Token) end);
  }

  @Override
  protected List<Token> split(Token startToken, Token endToken, int numberOfSplits) {
    // edge case: ]min, min] means the whole ring
    if (startToken.equals(endToken) && startToken.equals(Murmur3TokenFactory.MIN_TOKEN)) {
      endToken = Murmur3TokenFactory.MAX_TOKEN;
    }

    BigInteger start = BigInteger.valueOf(((Murmur3Token) startToken).getValue());
    BigInteger end = BigInteger.valueOf(((Murmur3Token) endToken).getValue());

    BigInteger range = end.subtract(start);
    if (range.compareTo(BigInteger.ZERO) < 0) {
      range = range.add(RING_LENGTH);
    }

    List<BigInteger> values = super.split(start, range, RING_END, RING_LENGTH, numberOfSplits);
    List<Token> tokens = Lists.newArrayListWithExpectedSize(values.size());
    for (BigInteger value : values) {
      tokens.add(new Murmur3Token(value.longValue()));
    }
    return tokens;
  }
}
