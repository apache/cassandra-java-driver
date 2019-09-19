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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class StatementBuilderTest {

  private static class NullStatementBuilder
      extends StatementBuilder<NullStatementBuilder, SimpleStatement> {

    public NullStatementBuilder() {
      super();
    }

    public NullStatementBuilder(SimpleStatement template) {
      super(template);
    }

    @Override
    public SimpleStatement build() {
      return null;
    }
  }

  @Test
  public void should_handle_set_tracing_without_args() {

    NullStatementBuilder builder = new NullStatementBuilder();
    assertFalse(builder.tracing);
    builder.setTracing();
    assertTrue(builder.tracing);
  }

  @Test
  public void should_handle_set_tracing_with_args() {

    NullStatementBuilder builder = new NullStatementBuilder();
    assertFalse(builder.tracing);
    builder.setTracing(true);
    assertTrue(builder.tracing);
    builder.setTracing(false);
    assertFalse(builder.tracing);
  }

  @Test
  public void should_override_template() {

    SimpleStatement template = SimpleStatement.builder("select * from system.peers").build();
    NullStatementBuilder builder = new NullStatementBuilder(template);
    assertFalse(builder.tracing);
    builder.setTracing(true);
    assertTrue(builder.tracing);

    template = SimpleStatement.builder("select * from system.peers").setTracing().build();
    builder = new NullStatementBuilder(template);
    assertTrue(builder.tracing);
    builder.setTracing(false);
    assertFalse(builder.tracing);
  }
}
