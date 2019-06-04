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
package com.datastax.oss.driver.api.querybuilder.truncate;

import static com.datastax.oss.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.truncate;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import org.junit.Test;

public class TruncateTest {

  @Test
  public void should_create_truncate_for_table_string() {
    assertThat(truncate("foo")).hasCql("TRUNCATE foo").isIdempotent();
  }

  @Test
  public void should_create_truncate_for_table_cql_identifier() {
    assertThat(truncate(CqlIdentifier.fromCql("foo"))).hasCql("TRUNCATE foo").isIdempotent();
  }

  @Test
  public void should_create_truncate_for_keyspace_and_table_string() {
    assertThat(truncate("ks", "foo")).hasCql("TRUNCATE ks.foo").isIdempotent();
  }

  @Test
  public void should_create_truncate_for_keyspace_and_table_cql_identifier() {
    assertThat(truncate(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("foo")))
        .hasCql("TRUNCATE ks.foo")
        .isIdempotent();
  }

  @Test
  public void should_create_truncate_if_call_build_without_arguments() {
    assertThat(
            truncate(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("foo")).build().getQuery())
        .isEqualTo("TRUNCATE ks.foo");
  }

  @Test
  public void should_throw_if_call_build_with_values() {
    assertThatThrownBy(
            () -> truncate(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("foo")).build("arg1"))
        .isExactlyInstanceOf(UnsupportedOperationException.class)
        .hasMessage("TRUNCATE doesn't take values as parameters. Use build() method instead.");
  }

  @Test
  public void should_throw_if_call_build_with_named_values() {
    assertThatThrownBy(
            () ->
                truncate(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("foo"))
                    .build(ImmutableMap.of("k", "v")))
        .isExactlyInstanceOf(UnsupportedOperationException.class)
        .hasMessage("TRUNCATE doesn't take namedValues as parameters. Use build() method instead.");
  }
}
