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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import java.util.List;
import org.assertj.core.api.AbstractAssert;

public class TokenRangeAssert extends AbstractAssert<TokenRangeAssert, TokenRange> {

  public TokenRangeAssert(TokenRange actual) {
    super(actual, TokenRangeAssert.class);
  }

  public TokenRangeAssert startsWith(Token token) {
    assertThat(actual.getStart()).isEqualTo(token);
    return this;
  }

  public TokenRangeAssert endsWith(Token token) {
    assertThat(actual.getEnd()).isEqualTo(token);
    return this;
  }

  public TokenRangeAssert isEmpty() {
    assertThat(actual.isEmpty()).isTrue();
    return this;
  }

  public TokenRangeAssert isNotEmpty() {
    assertThat(actual.isEmpty()).isFalse();
    return this;
  }

  public TokenRangeAssert isWrappedAround() {
    assertThat(actual.isWrappedAround()).isTrue();

    List<TokenRange> unwrapped = actual.unwrap();
    assertThat(unwrapped.size())
        .as("%s should unwrap to two ranges, but unwrapped to %s", actual, unwrapped)
        .isEqualTo(2);

    return this;
  }

  public TokenRangeAssert isNotWrappedAround() {
    assertThat(actual.isWrappedAround()).isFalse();
    assertThat(actual.unwrap()).containsExactly(actual);
    return this;
  }

  public TokenRangeAssert unwrapsTo(TokenRange... subRanges) {
    assertThat(actual.unwrap()).containsExactly(subRanges);
    return this;
  }

  public TokenRangeAssert intersects(TokenRange that) {
    assertThat(actual.intersects(that)).as("%s should intersect %s", actual, that).isTrue();
    assertThat(that.intersects(actual)).as("%s should intersect %s", that, actual).isTrue();
    return this;
  }

  public TokenRangeAssert doesNotIntersect(TokenRange... that) {
    for (TokenRange thatRange : that) {
      assertThat(actual.intersects(thatRange))
          .as("%s should not intersect %s", actual, thatRange)
          .isFalse();
      assertThat(thatRange.intersects(actual))
          .as("%s should not intersect %s", thatRange, actual)
          .isFalse();
    }
    return this;
  }

  public TokenRangeAssert contains(Token token, boolean isStart) {
    assertThat(((TokenRangeBase) actual).contains(actual, token, isStart)).isTrue();
    return this;
  }

  public TokenRangeAssert doesNotContain(Token token, boolean isStart) {
    assertThat(((TokenRangeBase) actual).contains(actual, token, isStart)).isFalse();
    return this;
  }
}
