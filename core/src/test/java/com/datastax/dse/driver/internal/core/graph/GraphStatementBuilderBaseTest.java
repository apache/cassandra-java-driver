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
package com.datastax.dse.driver.internal.core.graph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.api.core.graph.FluentGraphStatement;
import com.datastax.dse.driver.api.core.graph.GraphStatementBuilderBase;
import com.datastax.oss.driver.api.core.cql.Statement;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.junit.Test;

public class GraphStatementBuilderBaseTest {

  private static class MockGraphStatementBuilder
      extends GraphStatementBuilderBase<MockGraphStatementBuilder, FluentGraphStatement> {

    @NonNull
    @Override
    public FluentGraphStatement build() {
      FluentGraphStatement rv = mock(FluentGraphStatement.class);
      when(rv.getTimestamp()).thenReturn(this.timestamp);
      return rv;
    }
  }

  @Test
  public void should_use_timestamp_if_set() {

    MockGraphStatementBuilder builder = new MockGraphStatementBuilder();
    builder.setTimestamp(1);
    assertThat(builder.build().getTimestamp()).isEqualTo(1);
  }

  @Test
  public void should_use_correct_default_timestamp_if_not_set() {

    MockGraphStatementBuilder builder = new MockGraphStatementBuilder();
    assertThat(builder.build().getTimestamp()).isEqualTo(Statement.NO_DEFAULT_TIMESTAMP);
  }
}
