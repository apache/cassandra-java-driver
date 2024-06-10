/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.dse.driver.api.core.auth;

import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.util.Map;

public class ProxyAuthentication {
  private static final String PROXY_EXECUTE = "ProxyExecute";

  /**
   * Adds proxy authentication information to a CQL statement.
   *
   * <p>This allows executing a statement as another role than the one the session is currently
   * authenticated as.
   *
   * @param userOrRole the role to use for execution. If the statement was already configured with
   *     another role, it will get replaced by this one.
   * @param statement the statement to modify.
   * @return a statement that will run the same CQL query as {@code statement}, but acting as the
   *     provided role. Note: with the driver's default implementations, this will always be a copy;
   *     but if you use a custom implementation, it might return the same instance (depending on the
   *     behavior of {@link Statement#setCustomPayload(Map) statement.setCustomPayload()}).
   * @see <a
   *     href="https://docs.datastax.com/en/dse/6.0/dse-admin/datastax_enterprise/security/secProxy.html">Setting
   *     up roles for applications (DSE 6.0 admin guide)</a>
   */
  @NonNull
  public static <StatementT extends Statement<StatementT>> StatementT executeAs(
      @NonNull String userOrRole, @NonNull StatementT statement) {
    return statement.setCustomPayload(
        addProxyExecuteEntry(statement.getCustomPayload(), userOrRole));
  }

  /**
   * Adds proxy authentication information to a graph statement.
   *
   * @see #executeAs(String, Statement)
   */
  @NonNull
  public static <StatementT extends GraphStatement<StatementT>> StatementT executeAs(
      @NonNull String userOrRole, @NonNull StatementT statement) {
    return statement.setCustomPayload(
        addProxyExecuteEntry(statement.getCustomPayload(), userOrRole));
  }

  private static Map<String, ByteBuffer> addProxyExecuteEntry(
      Map<String, ByteBuffer> currentPayload, @NonNull String userOrRole) {
    NullAllowingImmutableMap.Builder<String, ByteBuffer> builder =
        NullAllowingImmutableMap.builder();
    builder.put(PROXY_EXECUTE, ByteBuffer.wrap(userOrRole.getBytes(Charsets.UTF_8)));
    if (!currentPayload.isEmpty()) {
      for (Map.Entry<String, ByteBuffer> entry : currentPayload.entrySet()) {
        String key = entry.getKey();
        if (!key.equals(PROXY_EXECUTE)) {
          builder.put(key, entry.getValue());
        }
      }
    }
    return builder.build();
  }
}
