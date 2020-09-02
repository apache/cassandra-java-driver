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
package com.datastax.oss.driver.internal.core.metadata.schema.queries;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class KeyspaceFilterTest {

  @Test
  public void should_not_filter_when_no_rules() {
    KeyspaceFilter filter = new KeyspaceFilter();
    assertThat(filter.getWhereClause()).isEmpty();
    assertThat(filter.includes("system")).isTrue();
    assertThat(filter.includes("ks1")).isTrue();
  }

  @Test
  public void should_filter_on_server_when_only_exact_inclusions_and_no_exclusions() {
    KeyspaceFilter filter = new KeyspaceFilter("ks1", "ks2");
    assertThat(filter.getWhereClause()).isEqualTo(" WHERE keyspace_name IN ('ks1','ks2')");
    assertThat(filter.includes("ks1")).isTrue();
    assertThat(filter.includes("ks2")).isTrue();
  }

  @Test
  public void should_filter_on_server_when_only_exact_inclusions_and_some_exclusions() {
    // The exclusion is useless, probably a user error (we log a warning)
    KeyspaceFilter filter = new KeyspaceFilter("ks1", "ks2", "!/KS.*/");
    assertThat(filter.getWhereClause()).isEqualTo(" WHERE keyspace_name IN ('ks1','ks2')");
    assertThat(filter.includes("ks1")).isTrue();
    assertThat(filter.includes("ks2")).isTrue();
    // The implementations honors the exclude client side, even though we'll never encounter that
    // keyspace given the where clause
    assertThat(filter.includes("KS1")).isFalse();
  }

  @Test
  public void should_filter_on_client_when_only_exclusions() {
    KeyspaceFilter filter = new KeyspaceFilter("!system");
    assertThat(filter.getWhereClause()).isEmpty();
    assertThat(filter.includes("system")).isFalse();
    assertThat(filter.includes("ks1")).isTrue();
  }

  @Test
  public void should_filter_on_client_when_regex_inclusions() {
    KeyspaceFilter filter = new KeyspaceFilter("foo1", "/bar.*/", "!BAR2", "!/.*3/");
    assertThat(filter.getWhereClause()).isEmpty();
    assertThat(filter.includes("foo1")).isTrue();
    assertThat(filter.includes("bar1")).isTrue();
    assertThat(filter.includes("bar2")).isTrue();
    assertThat(filter.includes("BAR2")).isFalse();
    assertThat(filter.includes("foo3")).isFalse();
  }

  @Test
  public void should_honor_exact_inclusion_over_any_other_rule() {
    KeyspaceFilter filter = new KeyspaceFilter("ks1", "!ks1", "!/ks.*/");
    assertThat(filter.getWhereClause()).isEqualTo(" WHERE keyspace_name IN ('ks1')");
    assertThat(filter.includes("ks1")).isTrue();

    filter = new KeyspaceFilter("ks1", "/.*1/", "!ks1", "!/ks.*/");
    assertThat(filter.getWhereClause()).isEmpty();
    assertThat(filter.includes("ks1")).isTrue();
  }

  @Test
  public void should_honor_exact_exclusion_over_regexes() {
    KeyspaceFilter filter = new KeyspaceFilter("!ks1", "/ks.*/");
    assertThat(filter.getWhereClause()).isEmpty();
    assertThat(filter.includes("ks1")).isFalse();
    assertThat(filter.includes("ks2")).isTrue();
  }

  @Test
  public void should_use_intersection_if_only_regexes() {
    KeyspaceFilter filter = new KeyspaceFilter("/ks.*/", "!/.*2/");
    assertThat(filter.getWhereClause()).isEmpty();
    // Matches the inclusion and not the exclusion
    assertThat(filter.includes("ks1")).isTrue();
    // Matches the inclusion but also the exclusion
    assertThat(filter.includes("ks2")).isFalse();
    // Does not match the exclusion, but neither the inclusion
    assertThat(filter.includes("foo1")).isFalse();

    // No inclusions == include "/.*/"
    filter = new KeyspaceFilter("!/.*2/");
    assertThat(filter.getWhereClause()).isEmpty();
    assertThat(filter.includes("ks1")).isTrue();
    assertThat(filter.includes("foo1")).isTrue();
    assertThat(filter.includes("ks2")).isFalse();
  }

  @Test
  public void should_skip_malformed_rule() {
    KeyspaceFilter filter = new KeyspaceFilter("ks1", "ks2", "//");
    assertThat(filter.getWhereClause()).isEqualTo(" WHERE keyspace_name IN ('ks1','ks2')");
    assertThat(filter.includes("ks1")).isTrue();
    assertThat(filter.includes("ks2")).isTrue();
  }

  @Test
  public void should_skip_invalid_regex() {
    KeyspaceFilter filter = new KeyspaceFilter("ks1", "ks2", "/*/");
    assertThat(filter.getWhereClause()).isEqualTo(" WHERE keyspace_name IN ('ks1','ks2')");
    assertThat(filter.includes("ks1")).isTrue();
    assertThat(filter.includes("ks2")).isTrue();
  }
}
