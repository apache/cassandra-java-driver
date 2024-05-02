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

import static com.datastax.oss.driver.internal.core.metadata.token.RandomTokenFactory.MAX_VALUE;

import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.math.BigInteger;
import java.util.List;
import net.jcip.annotations.Immutable;

@Immutable
public class RandomTokenRange extends TokenRangeBase {

  private static final BigInteger RING_LENGTH = MAX_VALUE.add(BigInteger.ONE);

  public RandomTokenRange(RandomToken start, RandomToken end) {
    super(start, end, RandomTokenFactory.MIN_TOKEN);
  }

  @Override
  protected TokenRange newTokenRange(Token start, Token end) {
    return new RandomTokenRange(((RandomToken) start), ((RandomToken) end));
  }

  @Override
  protected List<Token> split(Token startToken, Token endToken, int numberOfSplits) {
    // edge case: ]min, min] means the whole ring
    if (startToken.equals(endToken) && startToken.equals(RandomTokenFactory.MIN_TOKEN)) {
      endToken = RandomTokenFactory.MAX_TOKEN;
    }

    BigInteger start = ((RandomToken) startToken).getValue();
    BigInteger end = ((RandomToken) endToken).getValue();

    BigInteger range = end.subtract(start);
    if (range.compareTo(BigInteger.ZERO) < 0) {
      range = range.add(RING_LENGTH);
    }

    List<BigInteger> values = super.split(start, range, MAX_VALUE, RING_LENGTH, numberOfSplits);
    List<Token> tokens = Lists.newArrayListWithExpectedSize(values.size());
    for (BigInteger value : values) {
      tokens.add(new RandomToken(value));
    }
    return tokens;
  }
}
