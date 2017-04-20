/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.core.metadata;

import com.google.common.collect.ImmutableSet;
import java.net.InetSocketAddress;
import org.junit.Test;

import static com.datastax.oss.driver.Assertions.assertThat;

public class InitContactPointsRefreshTest {

  private static final InetSocketAddress ADDRESS1 = new InetSocketAddress("127.0.0.1", 9042);
  private static final InetSocketAddress ADDRESS2 = new InetSocketAddress("127.0.0.2", 9042);

  @Test
  public void should_create_nodes() {
    // Given
    InitContactPointsRefresh refresh =
        new InitContactPointsRefresh(ImmutableSet.of(ADDRESS1, ADDRESS2), "test");

    // When
    MetadataRefresh.Result result = refresh.compute(DefaultMetadata.EMPTY);

    // Then
    assertThat(result.newMetadata.getNodes()).containsOnlyKeys(ADDRESS1, ADDRESS2);
    assertThat(result.events).isEmpty();
  }
}
