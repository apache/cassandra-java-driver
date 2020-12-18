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
package com.datastax.oss.driver.api.querybuilder;

import static com.datastax.oss.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

import com.datastax.oss.driver.internal.core.metadata.token.ByteOrderedTokenFactory;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenFactory;
import com.datastax.oss.driver.internal.core.metadata.token.RandomTokenFactory;
import org.junit.Test;

public class TokenLiteralTest {

  @Test
  public void should_inline_murmur3_token_literal() {
    assertThat(
            selectFrom("test")
                .all()
                .whereToken("pk")
                .isEqualTo(literal(Murmur3TokenFactory.MIN_TOKEN)))
        .hasCql("SELECT * FROM test WHERE token(pk)=-9223372036854775808");
  }

  @Test
  public void should_inline_byte_ordered_token_literal() {
    assertThat(
            selectFrom("test")
                .all()
                .whereToken("pk")
                .isEqualTo(literal(ByteOrderedTokenFactory.MIN_TOKEN)))
        .hasCql("SELECT * FROM test WHERE token(pk)=0x");
  }

  @Test
  public void should_inline_random_token_literal() {
    assertThat(
            selectFrom("test")
                .all()
                .whereToken("pk")
                .isEqualTo(literal(RandomTokenFactory.MIN_TOKEN)))
        .hasCql("SELECT * FROM test WHERE token(pk)=-1");
  }
}
