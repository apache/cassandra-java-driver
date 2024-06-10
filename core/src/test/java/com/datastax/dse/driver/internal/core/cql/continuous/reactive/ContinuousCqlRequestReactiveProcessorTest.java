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
package com.datastax.dse.driver.internal.core.cql.continuous.reactive;

import static com.datastax.dse.driver.api.core.DseProtocolVersion.DSE_V1;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.DseTestDataProviders;
import com.datastax.dse.driver.DseTestFixtures;
import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.dse.driver.api.core.cql.continuous.reactive.ContinuousReactiveResultSet;
import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow;
import com.datastax.dse.driver.internal.core.cql.continuous.ContinuousCqlRequestAsyncProcessor;
import com.datastax.dse.driver.internal.core.cql.continuous.ContinuousCqlRequestHandlerTestBase;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.cql.PoolBehavior;
import com.datastax.oss.driver.internal.core.cql.RequestHandlerTestHarness;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import io.reactivex.Flowable;
import java.util.List;
import org.junit.Test;

public class ContinuousCqlRequestReactiveProcessorTest extends ContinuousCqlRequestHandlerTestBase {

  @Test
  public void should_be_able_to_process_reactive_result_set() {
    ContinuousCqlRequestReactiveProcessor processor =
        new ContinuousCqlRequestReactiveProcessor(new ContinuousCqlRequestAsyncProcessor());
    assertThat(
            processor.canProcess(
                UNDEFINED_IDEMPOTENCE_STATEMENT,
                ContinuousCqlRequestReactiveProcessor.CONTINUOUS_REACTIVE_RESULT_SET))
        .isTrue();
  }

  @Test
  public void should_create_request_handler() {
    RequestHandlerTestHarness.Builder builder =
        continuousHarnessBuilder().withProtocolVersion(DSE_V1);
    try (RequestHandlerTestHarness harness = builder.build()) {
      ContinuousCqlRequestReactiveProcessor processor =
          new ContinuousCqlRequestReactiveProcessor(new ContinuousCqlRequestAsyncProcessor());
      assertThat(
              processor.process(
                  UNDEFINED_IDEMPOTENCE_STATEMENT,
                  harness.getSession(),
                  harness.getContext(),
                  "test"))
          .isInstanceOf(DefaultContinuousReactiveResultSet.class);
    }
  }

  @Test
  @UseDataProvider(value = "allDseProtocolVersions", location = DseTestDataProviders.class)
  public void should_complete_single_page_result(DseProtocolVersion version) {
    try (RequestHandlerTestHarness harness =
        continuousHarnessBuilder()
            .withProtocolVersion(version)
            .withResponse(node1, defaultFrameOf(DseTestFixtures.singleDseRow()))
            .build()) {

      DefaultSession session = harness.getSession();
      InternalDriverContext context = harness.getContext();

      ContinuousReactiveResultSet publisher =
          new ContinuousCqlRequestReactiveProcessor(new ContinuousCqlRequestAsyncProcessor())
              .process(UNDEFINED_IDEMPOTENCE_STATEMENT, session, context, "test");

      List<ReactiveRow> rows = Flowable.fromPublisher(publisher).toList().blockingGet();

      assertThat(rows).hasSize(1);
      ReactiveRow row = rows.get(0);
      assertThat(row.getString("message")).isEqualTo("hello, world");
      ExecutionInfo executionInfo = row.getExecutionInfo();
      assertThat(executionInfo.getCoordinator()).isEqualTo(node1);
      assertThat(executionInfo.getErrors()).isEmpty();
      assertThat(executionInfo.getIncomingPayload()).isEmpty();
      assertThat(executionInfo.getPagingState()).isNull();
      assertThat(executionInfo.getSpeculativeExecutionCount()).isEqualTo(0);
      assertThat(executionInfo.getSuccessfulExecutionIndex()).isEqualTo(0);
      assertThat(executionInfo.getWarnings()).isEmpty();

      Flowable<ExecutionInfo> execInfosFlowable =
          Flowable.fromPublisher(publisher.getExecutionInfos());
      assertThat(execInfosFlowable.toList().blockingGet()).containsExactly(executionInfo);

      Flowable<ColumnDefinitions> colDefsFlowable =
          Flowable.fromPublisher(publisher.getColumnDefinitions());
      assertThat(colDefsFlowable.toList().blockingGet())
          .containsExactly(row.getColumnDefinitions());

      Flowable<Boolean> wasAppliedFlowable = Flowable.fromPublisher(publisher.wasApplied());
      assertThat(wasAppliedFlowable.toList().blockingGet()).containsExactly(row.wasApplied());
    }
  }

  @Test
  @UseDataProvider(value = "allDseProtocolVersions", location = DseTestDataProviders.class)
  public void should_complete_multi_page_result(DseProtocolVersion version) {
    RequestHandlerTestHarness.Builder builder =
        continuousHarnessBuilder().withProtocolVersion(version);
    PoolBehavior node1Behavior = builder.customBehavior(node1);
    try (RequestHandlerTestHarness harness = builder.build()) {

      DefaultSession session = harness.getSession();
      InternalDriverContext context = harness.getContext();

      ContinuousReactiveResultSet publisher =
          new ContinuousCqlRequestReactiveProcessor(new ContinuousCqlRequestAsyncProcessor())
              .process(UNDEFINED_IDEMPOTENCE_STATEMENT, session, context, "test");

      Flowable<ReactiveRow> rowsPublisher = Flowable.fromPublisher(publisher).cache();
      rowsPublisher.subscribe();

      node1Behavior.setResponseSuccess(defaultFrameOf(DseTestFixtures.tenDseRows(1, false)));
      node1Behavior.setResponseSuccess(defaultFrameOf(DseTestFixtures.tenDseRows(2, true)));

      List<ReactiveRow> rows = rowsPublisher.toList().blockingGet();
      assertThat(rows).hasSize(20);

      ReactiveRow first = rows.get(0);
      ExecutionInfo firstExecutionInfo = first.getExecutionInfo();
      assertThat(firstExecutionInfo.getCoordinator()).isEqualTo(node1);
      assertThat(firstExecutionInfo.getErrors()).isEmpty();
      assertThat(firstExecutionInfo.getIncomingPayload()).isEmpty();
      assertThat(firstExecutionInfo.getPagingState()).isNotNull();
      assertThat(firstExecutionInfo.getSpeculativeExecutionCount()).isEqualTo(0);
      assertThat(firstExecutionInfo.getSuccessfulExecutionIndex()).isEqualTo(0);
      assertThat(firstExecutionInfo.getWarnings()).isEmpty();

      ReactiveRow inSecondPage = rows.get(10);
      ExecutionInfo secondExecutionInfo = inSecondPage.getExecutionInfo();
      assertThat(secondExecutionInfo.getCoordinator()).isEqualTo(node1);
      assertThat(secondExecutionInfo.getErrors()).isEmpty();
      assertThat(secondExecutionInfo.getIncomingPayload()).isEmpty();
      assertThat(secondExecutionInfo.getPagingState()).isNull();
      assertThat(secondExecutionInfo.getSpeculativeExecutionCount()).isEqualTo(0);
      assertThat(secondExecutionInfo.getSuccessfulExecutionIndex()).isEqualTo(0);
      assertThat(secondExecutionInfo.getWarnings()).isEmpty();

      Flowable<ExecutionInfo> execInfosFlowable =
          Flowable.fromPublisher(publisher.getExecutionInfos());
      assertThat(execInfosFlowable.toList().blockingGet())
          .containsExactly(firstExecutionInfo, secondExecutionInfo);

      Flowable<ColumnDefinitions> colDefsFlowable =
          Flowable.fromPublisher(publisher.getColumnDefinitions());
      assertThat(colDefsFlowable.toList().blockingGet())
          .containsExactly(first.getColumnDefinitions());

      Flowable<Boolean> wasAppliedFlowable = Flowable.fromPublisher(publisher.wasApplied());
      assertThat(wasAppliedFlowable.toList().blockingGet()).containsExactly(first.wasApplied());
    }
  }
}
