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
package com.datastax.dse.driver;

import static com.datastax.dse.driver.internal.core.graph.GraphProtocol.GRAPHSON_1_0;
import static com.datastax.dse.driver.internal.core.graph.GraphProtocol.GRAPHSON_2_0;
import static com.datastax.dse.driver.internal.core.graph.GraphProtocol.GRAPH_BINARY_1_0;

import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.oss.driver.TestDataProviders;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.tngtech.java.junit.dataprovider.DataProvider;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Arrays;
import java.util.stream.Stream;

public class DseTestDataProviders {

  private static final ScriptGraphStatement UNDEFINED_IDEMPOTENCE_STATEMENT =
      ScriptGraphStatement.newInstance("undefined idempotence");
  private static final ScriptGraphStatement IDEMPOTENT_STATEMENT =
      ScriptGraphStatement.builder("idempotent").setIdempotence(true).build();
  private static final ScriptGraphStatement NON_IDEMPOTENT_STATEMENT =
      ScriptGraphStatement.builder("non idempotent").setIdempotence(false).build();

  @DataProvider
  public static Object[][] allDseProtocolVersions() {
    return concat(DseProtocolVersion.values());
  }

  @DataProvider
  public static Object[][] allOssProtocolVersions() {
    return concat(DefaultProtocolVersion.values());
  }

  @DataProvider
  public static Object[][] allDseAndOssProtocolVersions() {
    return concat(DefaultProtocolVersion.values(), DseProtocolVersion.values());
  }

  @DataProvider
  public static Object[][] supportedGraphProtocols() {
    return new Object[][] {{GRAPHSON_1_0}, {GRAPHSON_2_0}, {GRAPH_BINARY_1_0}};
  }

  /**
   * The combination of the default idempotence option and statement setting that produce an
   * idempotent statement.
   */
  @DataProvider
  public static Object[][] idempotentGraphConfig() {
    return new Object[][] {
      new Object[] {true, UNDEFINED_IDEMPOTENCE_STATEMENT},
      new Object[] {false, IDEMPOTENT_STATEMENT},
      new Object[] {true, IDEMPOTENT_STATEMENT},
    };
  }

  /**
   * The combination of the default idempotence option and statement setting that produce a non
   * idempotent statement.
   */
  @DataProvider
  public static Object[][] nonIdempotentGraphConfig() {
    return new Object[][] {
      new Object[] {false, UNDEFINED_IDEMPOTENCE_STATEMENT},
      new Object[] {true, NON_IDEMPOTENT_STATEMENT},
      new Object[] {false, NON_IDEMPOTENT_STATEMENT},
    };
  }

  @DataProvider
  public static Object[][] allDseProtocolVersionsAndSupportedGraphProtocols() {
    return TestDataProviders.combine(allDseProtocolVersions(), supportedGraphProtocols());
  }

  @NonNull
  private static Object[][] concat(Object[]... values) {
    return Stream.of(values)
        .flatMap(Arrays::stream)
        .map(o -> new Object[] {o})
        .toArray(Object[][]::new);
  }
}
