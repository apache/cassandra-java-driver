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
package com.datastax.oss.driver.api.core.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import java.nio.ByteBuffer;
import org.junit.Test;

public class StatementBuilderTest {

  private static class MockSimpleStatementBuilder
      extends StatementBuilder<MockSimpleStatementBuilder, SimpleStatement> {

    public MockSimpleStatementBuilder() {
      super();
    }

    public MockSimpleStatementBuilder(SimpleStatement template) {
      super(template);
    }

    @Override
    public SimpleStatement build() {

      SimpleStatement rv = mock(SimpleStatement.class);
      when(rv.isTracing()).thenReturn(this.tracing);
      when(rv.getRoutingKey()).thenReturn(this.routingKey);
      return rv;
    }
  }

  @Test
  public void should_handle_set_tracing_without_args() {

    MockSimpleStatementBuilder builder = new MockSimpleStatementBuilder();
    assertThat(builder.build().isTracing()).isFalse();
    builder.setTracing();
    assertThat(builder.build().isTracing()).isTrue();
  }

  @Test
  public void should_handle_set_tracing_with_args() {

    MockSimpleStatementBuilder builder = new MockSimpleStatementBuilder();
    assertThat(builder.build().isTracing()).isFalse();
    builder.setTracing(true);
    assertThat(builder.build().isTracing()).isTrue();
    builder.setTracing(false);
    assertThat(builder.build().isTracing()).isFalse();
  }

  @Test
  public void should_override_set_tracing_in_template() {

    SimpleStatement template = SimpleStatement.builder("select * from system.peers").build();
    MockSimpleStatementBuilder builder = new MockSimpleStatementBuilder(template);
    assertThat(builder.build().isTracing()).isFalse();
    builder.setTracing(true);
    assertThat(builder.build().isTracing()).isTrue();

    template = SimpleStatement.builder("select * from system.peers").setTracing().build();
    builder = new MockSimpleStatementBuilder(template);
    assertThat(builder.build().isTracing()).isTrue();
    builder.setTracing(false);
    assertThat(builder.build().isTracing()).isFalse();
  }

  @Test
  public void should_match_set_routing_key_vararg() {

    ByteBuffer buff1 = ByteBuffer.wrap("the quick brown fox".getBytes(Charsets.UTF_8));
    ByteBuffer buff2 = ByteBuffer.wrap("jumped over the lazy dog".getBytes(Charsets.UTF_8));

    Statement<?> expectedStmt =
        SimpleStatement.builder("select * from system.peers").build().setRoutingKey(buff1, buff2);

    MockSimpleStatementBuilder builder = new MockSimpleStatementBuilder();
    Statement<?> builderStmt = builder.setRoutingKey(buff1, buff2).build();
    assertThat(expectedStmt.getRoutingKey()).isEqualTo(builderStmt.getRoutingKey());

    /* Confirm that order matters here */
    builderStmt = builder.setRoutingKey(buff2, buff1).build();
    assertThat(expectedStmt.getRoutingKey()).isNotEqualTo(builderStmt.getRoutingKey());
  }
}
