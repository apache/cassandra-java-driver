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
package com.datastax.oss.driver.core.tracker;

import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.tracker.RequestIdGenerator;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class RequestIdGeneratorIT {
  private CcmRule ccmRule = CcmRule.getInstance();

  @Rule public TestRule chain = RuleChain.outerRule(ccmRule);

  @Test
  public void should_write_uuid_to_custom_payload_with_key() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.REQUEST_ID_GENERATOR_CLASS, "UuidRequestIdGenerator")
            .build();
    try (CqlSession session = SessionUtils.newSession(ccmRule, loader)) {
      String query = "SELECT * FROM system.local";
      ResultSet rs = session.execute(query);
      ByteBuffer id = rs.getExecutionInfo().getRequest().getCustomPayload().get("request-id");
      assertThat(id.remaining()).isEqualTo(73);
    }
  }

  @Test
  public void should_write_default_request_id_to_custom_payload_with_key() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(
                DefaultDriverOption.REQUEST_ID_GENERATOR_CLASS, "W3CContextRequestIdGenerator")
            .build();
    try (CqlSession session = SessionUtils.newSession(ccmRule, loader)) {
      String query = "SELECT * FROM system.local";
      ResultSet rs = session.execute(query);
      ByteBuffer id = rs.getExecutionInfo().getRequest().getCustomPayload().get("request-id");
      assertThat(id.remaining()).isEqualTo(55);
    }
  }

  @Test
  public void should_use_customized_request_id_generator() {
    RequestIdGenerator myRequestIdGenerator =
        new RequestIdGenerator() {
          @Override
          public String getSessionRequestId() {
            return "123";
          }

          @Override
          public String getNodeRequestId(@NonNull Request statement, @NonNull String parentId) {
            return "456";
          }

          @Override
          public Statement<?> getDecoratedStatement(
              @NonNull Statement<?> statement, @NonNull String requestId) {
            Map<String, ByteBuffer> customPayload =
                NullAllowingImmutableMap.<String, ByteBuffer>builder()
                    .putAll(statement.getCustomPayload())
                    .put("trace_key", ByteBuffer.wrap(requestId.getBytes(StandardCharsets.UTF_8)))
                    .build();
            return statement.setCustomPayload(customPayload);
          }
        };
    try (CqlSession session =
        (CqlSession)
            SessionUtils.baseBuilder()
                .addContactEndPoints(ccmRule.getContactPoints())
                .withRequestIdGenerator(myRequestIdGenerator)
                .build()) {
      String query = "SELECT * FROM system.local";
      ResultSet rs = session.execute(query);
      ByteBuffer id = rs.getExecutionInfo().getRequest().getCustomPayload().get("trace_key");
      assertThat(id).isEqualTo(ByteBuffer.wrap("456".getBytes(StandardCharsets.UTF_8)));
    }
  }

  @Test
  public void should_not_write_id_to_custom_payload_when_key_is_not_set() {
    DriverConfigLoader loader = SessionUtils.configLoaderBuilder().build();
    try (CqlSession session = SessionUtils.newSession(ccmRule, loader)) {
      String query = "SELECT * FROM system.local";
      ResultSet rs = session.execute(query);
      assertThat(rs.getExecutionInfo().getRequest().getCustomPayload().get("request-id")).isNull();
    }
  }

  @Test
  public void should_succeed_with_null_value_in_custom_payload() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(
                DefaultDriverOption.REQUEST_ID_GENERATOR_CLASS, "W3CContextRequestIdGenerator")
            .build();
    try (CqlSession session = SessionUtils.newSession(ccmRule, loader)) {
      String query = "SELECT * FROM system.local";
      Map<String, ByteBuffer> customPayload =
          new NullAllowingImmutableMap.Builder<String, ByteBuffer>(1).put("my_key", null).build();
      SimpleStatement statement =
          SimpleStatement.newInstance(query).setCustomPayload(customPayload);
      assertThatStage(session.executeAsync(statement)).isSuccess();
    }
  }
}
