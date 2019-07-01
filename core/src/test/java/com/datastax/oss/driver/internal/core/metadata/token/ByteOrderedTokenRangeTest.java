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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.protocol.internal.util.Bytes;
import org.junit.Test;

/** @see TokenRangeTest */
public class ByteOrderedTokenRangeTest {

  private static final String MIN = "0x";

  @Test
  public void should_split_range() {
    assertThat(range("0x0a", "0x0d").splitEvenly(3))
        .containsExactly(range("0x0a", "0x0b"), range("0x0b", "0x0c"), range("0x0c", "0x0d"));
  }

  @Test
  public void should_split_range_producing_empty_splits_near_ring_end() {
    // 0x00 is the first token following min.
    // This is an edge case where we want to make sure we don't accidentally generate the ]min,min]
    // range (which is the whole ring):
    assertThat(range(MIN, "0x00").splitEvenly(3))
        .containsExactly(range(MIN, "0x00"), range("0x00", "0x00"), range("0x00", "0x00"));
  }

  @Test
  public void should_split_range_when_padding_produces_same_token() {
    // To compute the ranges, we pad with trailing zeroes until the range is big enough for the
    // number of splits.
    // But in this case padding produces the same token 0x1100, so adding more zeroes wouldn't help.
    assertThat(range("0x11", "0x1100").splitEvenly(3))
        .containsExactly(
            range("0x11", "0x1100"), range("0x1100", "0x1100"), range("0x1100", "0x1100"));
  }

  @Test
  public void should_split_range_that_wraps_around_the_ring() {
    assertThat(range("0x0d", "0x0a").splitEvenly(2))
        .containsExactly(range("0x0d", "0x8c"), range("0x8c", "0x0a"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_split_whole_ring() {
    range(MIN, MIN).splitEvenly(1);
  }

  private ByteOrderedTokenRange range(String start, String end) {
    return new ByteOrderedTokenRange(
        new ByteOrderedToken(Bytes.fromHexString(start)),
        new ByteOrderedToken(Bytes.fromHexString(end)));
  }
}
