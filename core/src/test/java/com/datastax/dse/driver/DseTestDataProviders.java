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
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.tngtech.java.junit.dataprovider.DataProvider;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Arrays;
import java.util.stream.Stream;

public class DseTestDataProviders {

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

  @NonNull
  private static Object[][] concat(Object[]... values) {
    return Stream.of(values)
        .flatMap(Arrays::stream)
        .map(o -> new Object[] {o})
        .toArray(Object[][]::new);
  }
}
