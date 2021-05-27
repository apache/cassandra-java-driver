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

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;

public class KeyspaceFilterTest {

  private static final ImmutableSet<String> KEYSPACES =
      ImmutableSet.of(
          "system", "inventory_test", "inventory_prod", "customers_test", "customers_prod");

  @Test
  public void should_not_filter_when_no_rules() {
    KeyspaceFilter filter = KeyspaceFilter.newInstance("test", Arrays.asList());
    assertThat(filter.getWhereClause()).isEmpty();
    assertThat(apply(filter, KEYSPACES)).isEqualTo(KEYSPACES);
  }

  @Test
  public void should_filter_on_server_when_only_exact_rules() {
    KeyspaceFilter filter =
        KeyspaceFilter.newInstance(
            "test", Arrays.asList("inventory_test", "customers_test", "!system"));
    // Note that exact excludes are redundant in this case: either they match an include and will be
    // ignored, or they don't and the keyspace is already ignored.
    // We let it slide, but a warning is logged.
    assertThat(filter.getWhereClause())
        .isEqualTo(" WHERE keyspace_name IN ('inventory_test','customers_test')");
    assertThat(apply(filter, KEYSPACES)).containsOnly("inventory_test", "customers_test");
  }

  @Test
  public void should_ignore_exact_exclude_that_collides_with_exact_include() {
    KeyspaceFilter filter =
        KeyspaceFilter.newInstance("test", Arrays.asList("inventory_test", "!inventory_test"));
    assertThat(filter.getWhereClause()).isEqualTo(" WHERE keyspace_name IN ('inventory_test')");
    assertThat(apply(filter, KEYSPACES)).containsOnly("inventory_test");

    // Order does not matter
    filter = KeyspaceFilter.newInstance("test", Arrays.asList("!inventory_test", "inventory_test"));
    assertThat(filter.getWhereClause()).isEqualTo(" WHERE keyspace_name IN ('inventory_test')");
    assertThat(apply(filter, KEYSPACES)).containsOnly("inventory_test");
  }

  @Test
  public void should_apply_disjoint_exact_and_regex_rules() {
    KeyspaceFilter filter =
        KeyspaceFilter.newInstance("test", Arrays.asList("inventory_test", "/^customers.*/"));
    assertThat(filter.getWhereClause()).isEmpty();
    assertThat(apply(filter, KEYSPACES))
        .containsOnly("inventory_test", "customers_test", "customers_prod");

    filter = KeyspaceFilter.newInstance("test", Arrays.asList("!system", "!/^inventory.*/"));
    assertThat(filter.getWhereClause()).isEmpty();
    assertThat(apply(filter, KEYSPACES)).containsOnly("customers_test", "customers_prod");

    // The remaining cases could be simplified, but they are supported nevertheless:
    /*redundant:*/
    filter = KeyspaceFilter.newInstance("test", Arrays.asList("!/^customers.*/", "inventory_test"));
    assertThat(filter.getWhereClause()).isEmpty();
    assertThat(apply(filter, KEYSPACES)).containsOnly("inventory_test", "inventory_prod", "system");

    /*redundant:*/
    filter = KeyspaceFilter.newInstance("test", Arrays.asList("/^customers.*/", "!system"));
    assertThat(filter.getWhereClause()).isEmpty();
    assertThat(apply(filter, KEYSPACES)).containsOnly("customers_test", "customers_prod");
  }

  @Test
  public void should_apply_intersecting_exact_and_regex_rules() {
    // Include all customer keyspaces except one:
    KeyspaceFilter filter =
        KeyspaceFilter.newInstance("test", Arrays.asList("/^customers.*/", "!customers_test"));
    assertThat(filter.getWhereClause()).isEmpty();
    assertThat(apply(filter, KEYSPACES)).containsOnly("customers_prod");

    // Exclude all customer keyspaces except one (also implies include every other keyspace):
    filter = KeyspaceFilter.newInstance("test", Arrays.asList("!/^customers.*/", "customers_test"));
    assertThat(filter.getWhereClause()).isEmpty();
    assertThat(apply(filter, KEYSPACES))
        .containsOnly("customers_test", "inventory_test", "inventory_prod", "system");
  }

  @Test
  public void should_apply_intersecting_regex_rules() {
    KeyspaceFilter filter =
        KeyspaceFilter.newInstance("test", Arrays.asList("/^customers.*/", "!/.*test$/"));
    assertThat(filter.getWhereClause()).isEmpty();
    assertThat(apply(filter, KEYSPACES)).containsOnly("customers_prod");

    // Throwing an exact name in the mix doesn't change the other rules
    filter =
        KeyspaceFilter.newInstance(
            "test", Arrays.asList("inventory_prod", "/^customers.*/", "!/.*test$/"));
    assertThat(filter.getWhereClause()).isEmpty();
    assertThat(apply(filter, KEYSPACES)).containsOnly("inventory_prod", "customers_prod");
  }

  @Test
  public void should_skip_malformed_rule() {
    KeyspaceFilter filter =
        KeyspaceFilter.newInstance("test", Arrays.asList("inventory_test", "customers_test", "//"));
    assertThat(filter.getWhereClause())
        .isEqualTo(" WHERE keyspace_name IN ('inventory_test','customers_test')");
    assertThat(apply(filter, KEYSPACES)).containsOnly("inventory_test", "customers_test");
  }

  @Test
  public void should_skip_invalid_regex() {
    KeyspaceFilter filter =
        KeyspaceFilter.newInstance(
            "test", Arrays.asList("inventory_test", "customers_test", "/*/"));
    assertThat(filter.getWhereClause())
        .isEqualTo(" WHERE keyspace_name IN ('inventory_test','customers_test')");
    assertThat(apply(filter, KEYSPACES)).containsOnly("inventory_test", "customers_test");
  }

  private static Set<String> apply(KeyspaceFilter filter, Set<String> keyspaces) {
    return keyspaces.stream().filter(filter::includes).collect(Collectors.toSet());
  }
}
