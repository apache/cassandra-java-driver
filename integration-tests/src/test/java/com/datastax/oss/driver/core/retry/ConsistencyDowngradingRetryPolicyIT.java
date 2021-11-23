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
package com.datastax.oss.driver.core.retry;

import static com.datastax.oss.simulacron.common.codec.WriteType.BATCH_LOG;
import static com.datastax.oss.simulacron.common.codec.WriteType.UNLOGGED_BATCH;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.closeConnection;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.readFailure;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.readTimeout;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.serverError;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.unavailable;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.writeFailure;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.writeTimeout;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.retry.RetryVerdict;
import com.datastax.oss.driver.api.core.servererrors.DefaultWriteType;
import com.datastax.oss.driver.api.core.servererrors.ReadFailureException;
import com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.ServerError;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import com.datastax.oss.driver.api.testinfra.loadbalancing.SortingLoadBalancingPolicy;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.QueryCounter;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.retry.ConsistencyDowngradingRetryPolicy;
import com.datastax.oss.driver.internal.core.retry.ConsistencyDowngradingRetryVerdict;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.codec.WriteType;
import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.common.request.Request;
import com.datastax.oss.simulacron.common.stubbing.CloseType;
import com.datastax.oss.simulacron.common.stubbing.DisconnectAction;
import com.datastax.oss.simulacron.server.BoundNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

@RunWith(DataProviderRunner.class)
@Category(ParallelizableTests.class)
public class ConsistencyDowngradingRetryPolicyIT {

  @ClassRule
  public static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(3));

  public @Rule SessionRule<CqlSession> sessionRule =
      SessionRule.builder(SIMULACRON_RULE)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withBoolean(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE, true)
                  .withClass(
                      DefaultDriverOption.RETRY_POLICY_CLASS,
                      ConsistencyDowngradingRetryPolicy.class)
                  .withClass(
                      DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
                      SortingLoadBalancingPolicy.class)
                  .build())
          .build();

  private static final String QUERY_STR = "irrelevant";

  private static final Request QUERY_LOCAL_QUORUM =
      new Query(QUERY_STR, ImmutableList.of(ConsistencyLevel.LOCAL_QUORUM), null, null);

  private static final Request QUERY_ONE =
      new Query(QUERY_STR, ImmutableList.of(ConsistencyLevel.ONE), null, null);

  private static final Request QUERY_LOCAL_SERIAL =
      new Query(QUERY_STR, ImmutableList.of(ConsistencyLevel.LOCAL_SERIAL), null, null);

  private static final SimpleStatement STATEMENT_LOCAL_QUORUM =
      SimpleStatement.builder(QUERY_STR)
          .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
          .build();

  private static final SimpleStatement STATEMENT_LOCAL_SERIAL =
      SimpleStatement.builder(QUERY_STR)
          .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_SERIAL)
          .build();

  private final QueryCounter localQuorumCounter =
      QueryCounter.builder(SIMULACRON_RULE.cluster())
          .withFilter(
              (l) ->
                  l.getQuery().equals(QUERY_STR)
                      && l.getConsistency().equals(ConsistencyLevel.LOCAL_QUORUM))
          .build();

  private final QueryCounter oneCounter =
      QueryCounter.builder(SIMULACRON_RULE.cluster())
          .withFilter(
              (l) ->
                  l.getQuery().equals(QUERY_STR) && l.getConsistency().equals(ConsistencyLevel.ONE))
          .build();

  private final QueryCounter localSerialCounter =
      QueryCounter.builder(SIMULACRON_RULE.cluster())
          .withFilter(
              (l) ->
                  l.getQuery().equals(QUERY_STR)
                      && l.getConsistency().equals(ConsistencyLevel.LOCAL_SERIAL))
          .build();

  private ArgumentCaptor<ILoggingEvent> loggingEventCaptor;
  private Appender<ILoggingEvent> appender;
  private Logger logger;
  private Level oldLevel;
  private String logPrefix;
  private BoundNode node0;
  private BoundNode node1;

  @Before
  public void setup() {
    loggingEventCaptor = ArgumentCaptor.forClass(ILoggingEvent.class);
    @SuppressWarnings("unchecked")
    Appender<ILoggingEvent> appender = (Appender<ILoggingEvent>) mock(Appender.class);
    this.appender = appender;
    logger = (Logger) LoggerFactory.getLogger(ConsistencyDowngradingRetryPolicy.class);
    oldLevel = logger.getLevel();
    logger.setLevel(Level.TRACE);
    logger.addAppender(appender);
    // the log prefix we expect in retry logging messages.
    logPrefix = sessionRule.session().getName() + "|default";
    // clear activity logs and primes between tests since simulacron instance is shared.
    SIMULACRON_RULE.cluster().clearLogs();
    SIMULACRON_RULE.cluster().clearPrimes(true);
    node0 = SIMULACRON_RULE.cluster().node(0);
    node1 = SIMULACRON_RULE.cluster().node(1);
  }

  @After
  public void teardown() {
    logger.detachAppender(appender);
    logger.setLevel(oldLevel);
  }

  @Test
  public void should_rethrow_on_read_timeout_when_enough_responses_and_data_present() {
    // given a node that will respond to query with a read timeout where data is present and enough
    // replicas replied.
    node0.prime(
        when(QUERY_LOCAL_QUORUM).then(readTimeout(ConsistencyLevel.LOCAL_QUORUM, 2, 2, true)));

    try {
      sessionRule.session().execute(STATEMENT_LOCAL_QUORUM);
      fail("expected a ReadTimeoutException");
    } catch (ReadTimeoutException rte) {
      // then an exception should have been thrown
      assertThat(rte)
          .hasMessageContaining(
              "Cassandra timeout during read query at consistency LOCAL_QUORUM (timeout while waiting for repair of inconsistent replica)");
      assertThat(rte.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.LOCAL_QUORUM);
      assertThat(rte.getReceived()).isEqualTo(2);
      assertThat(rte.getBlockFor()).isEqualTo(2);
      assertThat(rte.wasDataPresent()).isTrue();
      // should not have been retried
      List<Entry<Node, Throwable>> errors = rte.getExecutionInfo().getErrors();
      assertThat(errors).isEmpty();
      // the host that returned the response should be node 0.
      assertThat(coordinatorAddress(rte.getExecutionInfo())).isEqualTo(node0.getAddress());
    }

    // there should have been no retry.
    localQuorumCounter.assertTotalCount(1);
    localQuorumCounter.assertNodeCounts(1, 0, 0);
    oneCounter.assertTotalCount(0);

    // expect 1 message: RETHROW
    verify(appender, timeout(500)).doAppend(loggingEventCaptor.capture());
    List<ILoggingEvent> loggedEvents = loggingEventCaptor.getAllValues();
    assertThat(loggedEvents).hasSize(1);
    assertThat(loggedEvents.get(0).getFormattedMessage())
        .isEqualTo(
            expectedMessage(
                ConsistencyDowngradingRetryPolicy.VERDICT_ON_READ_TIMEOUT,
                logPrefix,
                DefaultConsistencyLevel.LOCAL_QUORUM,
                2,
                2,
                true,
                0,
                RetryVerdict.RETHROW));
  }

  @Test
  public void should_retry_on_same_on_read_timeout_when_enough_responses_but_data_not_present() {
    // given a node that will respond to query with a read timeout where data is present.
    node0.prime(
        when(QUERY_LOCAL_QUORUM).then(readTimeout(ConsistencyLevel.LOCAL_QUORUM, 2, 2, false)));

    try {
      sessionRule.session().execute(STATEMENT_LOCAL_QUORUM);
      fail("expected a ReadTimeoutException");
    } catch (ReadTimeoutException rte) {
      // then an exception should have been thrown
      assertThat(rte)
          .hasMessageContaining(
              "Cassandra timeout during read query at consistency LOCAL_QUORUM (the replica queried for data didn't respond)");
      assertThat(rte.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.LOCAL_QUORUM);
      assertThat(rte.getReceived()).isEqualTo(2);
      assertThat(rte.getBlockFor()).isEqualTo(2);
      assertThat(rte.wasDataPresent()).isFalse();
      // the host that returned the response should be node 0.
      assertThat(coordinatorAddress(rte.getExecutionInfo())).isEqualTo(node0.getAddress());
      // should have failed at first attempt at LOCAL_QUORUM as well
      List<Entry<Node, Throwable>> errors = rte.getExecutionInfo().getErrors();
      assertThat(errors).hasSize(1);
      Entry<Node, Throwable> error = errors.get(0);
      assertThat(error.getKey().getEndPoint().resolve()).isEqualTo(node0.getAddress());
      assertThat(error.getValue())
          .isInstanceOfSatisfying(
              ReadTimeoutException.class,
              rte1 -> {
                assertThat(rte1)
                    .hasMessageContaining(
                        "Cassandra timeout during read query at consistency LOCAL_QUORUM (the replica queried for data didn't respond)");
                assertThat(rte1.getConsistencyLevel())
                    .isEqualTo(DefaultConsistencyLevel.LOCAL_QUORUM);
                assertThat(rte1.getReceived()).isEqualTo(2);
                assertThat(rte1.getBlockFor()).isEqualTo(2);
                assertThat(rte1.wasDataPresent()).isFalse();
              });
    }

    // there should have been a retry, and it should have been executed on the same host,
    // with same consistency.
    localQuorumCounter.assertTotalCount(2);
    localQuorumCounter.assertNodeCounts(2, 0, 0);
    oneCounter.assertTotalCount(0);

    // expect 2 messages: RETRY_SAME, then RETHROW
    verify(appender, timeout(2000).times(2)).doAppend(loggingEventCaptor.capture());
    List<ILoggingEvent> loggedEvents = loggingEventCaptor.getAllValues();
    assertThat(loggedEvents).hasSize(2);
    assertThat(loggedEvents.get(0).getFormattedMessage())
        .isEqualTo(
            expectedMessage(
                ConsistencyDowngradingRetryPolicy.VERDICT_ON_READ_TIMEOUT,
                logPrefix,
                DefaultConsistencyLevel.LOCAL_QUORUM,
                2,
                2,
                false,
                0,
                RetryVerdict.RETRY_SAME));
    assertThat(loggedEvents.get(1).getFormattedMessage())
        .isEqualTo(
            expectedMessage(
                ConsistencyDowngradingRetryPolicy.VERDICT_ON_READ_TIMEOUT,
                logPrefix,
                DefaultConsistencyLevel.LOCAL_QUORUM,
                2,
                2,
                false,
                1,
                RetryVerdict.RETHROW));
  }

  @Test
  public void should_downgrade_on_read_timeout_when_not_enough_responses() {
    // given a node that will respond to a query with a read timeout where 2 out of 3 responses are
    // received. In this case, digest requests succeeded, but not the data request.
    node0.prime(
        when(QUERY_LOCAL_QUORUM).then(readTimeout(ConsistencyLevel.LOCAL_QUORUM, 1, 2, true)));

    ResultSet rs = sessionRule.session().execute(STATEMENT_LOCAL_QUORUM);

    // the host that returned the response should be node 0.
    assertThat(coordinatorAddress(rs.getExecutionInfo())).isEqualTo(node0.getAddress());
    // should have failed at first attempt at LOCAL_QUORUM
    List<Entry<Node, Throwable>> errors = rs.getExecutionInfo().getErrors();
    assertThat(errors).hasSize(1);
    Entry<Node, Throwable> error = errors.get(0);
    assertThat(error.getKey().getEndPoint().resolve()).isEqualTo(node0.getAddress());
    assertThat(error.getValue())
        .isInstanceOfSatisfying(
            ReadTimeoutException.class,
            rte -> {
              assertThat(rte)
                  .hasMessageContaining(
                      "Cassandra timeout during read query at consistency LOCAL_QUORUM (2 responses were required but only 1 replica responded)");
              assertThat(rte.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.LOCAL_QUORUM);
              assertThat(rte.getReceived()).isEqualTo(1);
              assertThat(rte.getBlockFor()).isEqualTo(2);
              assertThat(rte.wasDataPresent()).isTrue();
            });

    // should have succeeded in second attempt at ONE
    Statement<?> request = (Statement<?>) rs.getExecutionInfo().getRequest();
    assertThat(request.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.ONE);

    // there should have been a retry, and it should have been executed on the same host,
    // but with consistency ONE.
    localQuorumCounter.assertTotalCount(1);
    localQuorumCounter.assertNodeCounts(1, 0, 0);
    oneCounter.assertTotalCount(1);
    oneCounter.assertNodeCounts(1, 0, 0);

    // expect 1 message: RETRY_SAME with ONE
    verify(appender, timeout(500)).doAppend(loggingEventCaptor.capture());
    List<ILoggingEvent> loggedEvents = loggingEventCaptor.getAllValues();
    assertThat(loggedEvents).hasSize(1);
    assertThat(loggedEvents.get(0).getFormattedMessage())
        .isEqualTo(
            expectedMessage(
                ConsistencyDowngradingRetryPolicy.VERDICT_ON_READ_TIMEOUT,
                logPrefix,
                DefaultConsistencyLevel.LOCAL_QUORUM,
                2,
                1,
                true,
                0,
                new ConsistencyDowngradingRetryVerdict(DefaultConsistencyLevel.ONE)));
  }

  @Test
  public void should_retry_on_read_timeout_when_enough_responses_and_data_not_present() {
    // given a node that will respond to a query with a read timeout where 3 out of 3 responses are
    // received, but data is not present.
    node0.prime(
        when(QUERY_LOCAL_QUORUM).then(readTimeout(ConsistencyLevel.LOCAL_QUORUM, 2, 2, false)));

    try {
      // when executing a query.
      sessionRule.session().execute(STATEMENT_LOCAL_QUORUM);
      fail("Expected a ReadTimeoutException");
    } catch (ReadTimeoutException rte) {
      // then a read timeout exception is thrown.
      assertThat(rte)
          .hasMessageContaining(
              "Cassandra timeout during read query at consistency LOCAL_QUORUM (the replica queried for data didn't respond)");
      assertThat(rte.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.LOCAL_QUORUM);
      assertThat(rte.getReceived()).isEqualTo(2);
      assertThat(rte.getBlockFor()).isEqualTo(2);
      assertThat(rte.wasDataPresent()).isFalse();
      // the host that returned the response should be node 0.
      assertThat(coordinatorAddress(rte.getExecutionInfo())).isEqualTo(node0.getAddress());
      // should have failed at first attempt at LOCAL_QUORUM
      List<Entry<Node, Throwable>> errors = rte.getExecutionInfo().getErrors();
      assertThat(errors).hasSize(1);
      Entry<Node, Throwable> error = errors.get(0);
      assertThat(error.getKey().getEndPoint().resolve()).isEqualTo(node0.getAddress());
      assertThat(error.getValue())
          .isInstanceOfSatisfying(
              ReadTimeoutException.class,
              rte1 -> {
                assertThat(rte)
                    .hasMessageContaining(
                        "Cassandra timeout during read query at consistency LOCAL_QUORUM (the replica queried for data didn't respond)");
                assertThat(rte1.getConsistencyLevel())
                    .isEqualTo(DefaultConsistencyLevel.LOCAL_QUORUM);
                assertThat(rte1.getReceived()).isEqualTo(2);
                assertThat(rte1.getBlockFor()).isEqualTo(2);
                assertThat(rte1.wasDataPresent()).isFalse();
              });
    }

    // there should have been a retry, and it should have been executed on the same host.
    localQuorumCounter.assertTotalCount(2);
    localQuorumCounter.assertNodeCounts(2, 0, 0);
    oneCounter.assertTotalCount(0);

    // verify log events were emitted as expected
    verify(appender, timeout(500).times(2)).doAppend(loggingEventCaptor.capture());
    List<ILoggingEvent> loggedEvents = loggingEventCaptor.getAllValues();
    assertThat(loggedEvents).hasSize(2);
    assertThat(loggedEvents.get(0).getFormattedMessage())
        .isEqualTo(
            expectedMessage(
                ConsistencyDowngradingRetryPolicy.VERDICT_ON_READ_TIMEOUT,
                logPrefix,
                DefaultConsistencyLevel.LOCAL_QUORUM,
                2,
                2,
                false,
                0,
                RetryVerdict.RETRY_SAME));
    assertThat(loggedEvents.get(1).getFormattedMessage())
        .isEqualTo(
            expectedMessage(
                ConsistencyDowngradingRetryPolicy.VERDICT_ON_READ_TIMEOUT,
                logPrefix,
                DefaultConsistencyLevel.LOCAL_QUORUM,
                2,
                2,
                false,
                1,
                RetryVerdict.RETHROW));
  }

  @Test
  public void should_only_retry_once_on_read_type() {
    // given a node that will respond to a query with a read timeout at 2 CLs.
    node0.prime(
        when(QUERY_LOCAL_QUORUM).then(readTimeout(ConsistencyLevel.LOCAL_QUORUM, 1, 2, true)));
    node0.prime(when(QUERY_ONE).then(readTimeout(ConsistencyLevel.ONE, 0, 1, false)));
    try {
      // when executing a query.
      sessionRule.session().execute(STATEMENT_LOCAL_QUORUM);
      fail("Expected a ReadTimeoutException");
    } catch (ReadTimeoutException wte) {
      // then a read timeout exception is thrown
      assertThat(wte)
          .hasMessageContaining(
              "Cassandra timeout during read query at consistency ONE (1 responses were required but only 0 replica responded)");
      assertThat(wte.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.ONE);
      assertThat(wte.getReceived()).isEqualTo(0);
      assertThat(wte.getBlockFor()).isEqualTo(1);
      assertThat(wte.wasDataPresent()).isFalse();
      // the host that returned the response should be node 0.
      assertThat(coordinatorAddress(wte.getExecutionInfo())).isEqualTo(node0.getAddress());
      // should have failed at first attempt at LOCAL_QUORUM as well
      List<Entry<Node, Throwable>> errors = wte.getExecutionInfo().getErrors();
      assertThat(errors).hasSize(1);
      Entry<Node, Throwable> error = errors.get(0);
      assertThat(error.getKey().getEndPoint().resolve()).isEqualTo(node0.getAddress());
      assertThat(error.getValue())
          .isInstanceOfSatisfying(
              ReadTimeoutException.class,
              wte1 -> {
                assertThat(wte1)
                    .hasMessageContaining(
                        "Cassandra timeout during read query at consistency LOCAL_QUORUM (2 responses were required but only 1 replica responded)");
                assertThat(wte1.getConsistencyLevel())
                    .isEqualTo(DefaultConsistencyLevel.LOCAL_QUORUM);
                assertThat(wte1.getReceived()).isEqualTo(1);
                assertThat(wte1.getBlockFor()).isEqualTo(2);
                assertThat(wte1.wasDataPresent()).isTrue();
              });
    }

    // should have been retried on same host, but at consistency ONE.
    localQuorumCounter.assertTotalCount(1);
    localQuorumCounter.assertNodeCounts(1, 0, 0);
    oneCounter.assertTotalCount(1);
    oneCounter.assertNodeCounts(1, 0, 0);

    // verify log events were emitted as expected
    verify(appender, timeout(500).times(2)).doAppend(loggingEventCaptor.capture());
    List<ILoggingEvent> loggedEvents = loggingEventCaptor.getAllValues();
    assertThat(loggedEvents).hasSize(2);
    assertThat(loggedEvents.get(0).getFormattedMessage())
        .isEqualTo(
            expectedMessage(
                ConsistencyDowngradingRetryPolicy.VERDICT_ON_READ_TIMEOUT,
                logPrefix,
                DefaultConsistencyLevel.LOCAL_QUORUM,
                2,
                1,
                true,
                0,
                new ConsistencyDowngradingRetryVerdict(DefaultConsistencyLevel.ONE)));
    assertThat(loggedEvents.get(1).getFormattedMessage())
        .isEqualTo(
            expectedMessage(
                ConsistencyDowngradingRetryPolicy.VERDICT_ON_READ_TIMEOUT,
                logPrefix,
                DefaultConsistencyLevel.ONE,
                1,
                0,
                false,
                1,
                RetryVerdict.RETHROW));
  }

  @Test
  public void should_retry_on_write_timeout_if_write_type_batch_log() {
    // given a node that will respond to query with a write timeout with write type of batch log.
    node0.prime(
        when(QUERY_LOCAL_QUORUM)
            .then(writeTimeout(ConsistencyLevel.LOCAL_QUORUM, 1, 2, BATCH_LOG)));

    try {
      // when executing a query.
      sessionRule.session().execute(STATEMENT_LOCAL_QUORUM);
      fail("WriteTimeoutException expected");
    } catch (WriteTimeoutException wte) {
      // then a write timeout exception is thrown
      assertThat(wte)
          .hasMessageContaining(
              "Cassandra timeout during BATCH_LOG write query at consistency LOCAL_QUORUM (2 replica were required but only 1 acknowledged the write)");
      assertThat(wte.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.LOCAL_QUORUM);
      assertThat(wte.getReceived()).isEqualTo(1);
      assertThat(wte.getBlockFor()).isEqualTo(2);
      assertThat(wte.getWriteType()).isEqualTo(DefaultWriteType.BATCH_LOG);
      // the host that returned the response should be node 0.
      assertThat(coordinatorAddress(wte.getExecutionInfo())).isEqualTo(node0.getAddress());
      // should have failed at first attempt at LOCAL_QUORUM as well
      List<Entry<Node, Throwable>> errors = wte.getExecutionInfo().getErrors();
      assertThat(errors).hasSize(1);
      Entry<Node, Throwable> error = errors.get(0);
      assertThat(error.getKey().getEndPoint().resolve()).isEqualTo(node0.getAddress());
      assertThat(error.getValue())
          .isInstanceOfSatisfying(
              WriteTimeoutException.class,
              wte1 -> {
                assertThat(wte1)
                    .hasMessageContaining(
                        "Cassandra timeout during BATCH_LOG write query at consistency LOCAL_QUORUM (2 replica were required but only 1 acknowledged the write)");
                assertThat(wte1.getConsistencyLevel())
                    .isEqualTo(DefaultConsistencyLevel.LOCAL_QUORUM);
                assertThat(wte1.getReceived()).isEqualTo(1);
                assertThat(wte1.getBlockFor()).isEqualTo(2);
                assertThat(wte1.getWriteType()).isEqualTo(DefaultWriteType.BATCH_LOG);
              });
    }

    // there should have been a retry, and it should have been executed on the same host.
    localQuorumCounter.assertTotalCount(2);
    localQuorumCounter.assertNodeCounts(2, 0, 0);
    oneCounter.assertTotalCount(0);

    // verify log events were emitted as expected
    verify(appender, timeout(500).times(2)).doAppend(loggingEventCaptor.capture());
    List<ILoggingEvent> loggedEvents = loggingEventCaptor.getAllValues();
    assertThat(loggedEvents).hasSize(2);
    assertThat(loggedEvents.get(0).getFormattedMessage())
        .isEqualTo(
            expectedMessage(
                ConsistencyDowngradingRetryPolicy.VERDICT_ON_WRITE_TIMEOUT,
                logPrefix,
                DefaultConsistencyLevel.LOCAL_QUORUM,
                DefaultWriteType.BATCH_LOG,
                2,
                1,
                0,
                RetryVerdict.RETRY_SAME));
    assertThat(loggedEvents.get(1).getFormattedMessage())
        .isEqualTo(
            expectedMessage(
                ConsistencyDowngradingRetryPolicy.VERDICT_ON_WRITE_TIMEOUT,
                logPrefix,
                DefaultConsistencyLevel.LOCAL_QUORUM,
                DefaultWriteType.BATCH_LOG,
                2,
                1,
                1,
                RetryVerdict.RETHROW));
  }

  @Test
  public void should_not_retry_on_write_timeout_if_write_type_batch_log_but_non_idempotent() {
    // given a node that will respond to query with a write timeout with write type of batch log.
    node0.prime(
        when(QUERY_LOCAL_QUORUM)
            .then(writeTimeout(ConsistencyLevel.LOCAL_QUORUM, 1, 2, BATCH_LOG)));

    try {
      // when executing a non-idempotent query.
      sessionRule.session().execute(STATEMENT_LOCAL_QUORUM.setIdempotent(false));
      fail("WriteTimeoutException expected");
    } catch (WriteTimeoutException wte) {
      // then a write timeout exception is thrown
      assertThat(wte)
          .hasMessageContaining(
              "Cassandra timeout during BATCH_LOG write query at consistency LOCAL_QUORUM (2 replica were required but only 1 acknowledged the write)");
      assertThat(wte.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.LOCAL_QUORUM);
      assertThat(wte.getReceived()).isEqualTo(1);
      assertThat(wte.getBlockFor()).isEqualTo(2);
      assertThat(wte.getWriteType()).isEqualTo(DefaultWriteType.BATCH_LOG);
      // the host that returned the response should be node 0.
      assertThat(coordinatorAddress(wte.getExecutionInfo())).isEqualTo(node0.getAddress());
      // should not have been retried
      List<Entry<Node, Throwable>> errors = wte.getExecutionInfo().getErrors();
      assertThat(errors).isEmpty();
    }

    // should not have been retried.
    localQuorumCounter.assertTotalCount(1);
    oneCounter.assertTotalCount(0);

    // expect no logging messages since there was no retry
    verify(appender, after(500).times(0)).doAppend(any(ILoggingEvent.class));
  }

  @DataProvider({"SIMPLE,SIMPLE", "BATCH,BATCH"})
  @Test
  public void should_ignore_on_write_timeout_if_write_type_ignorable_and_at_least_one_ack_received(
      WriteType writeType, DefaultWriteType driverWriteType) {
    // given a node that will respond to query with a write timeout with write type that is either
    // SIMPLE or BATCH.
    node0.prime(
        when(QUERY_LOCAL_QUORUM)
            .then(writeTimeout(ConsistencyLevel.LOCAL_QUORUM, 1, 2, writeType)));

    // when executing a query.
    ResultSet rs = sessionRule.session().execute(STATEMENT_LOCAL_QUORUM);

    // should have ignored the write timeout
    assertThat(rs.all()).isEmpty();
    // the host that returned the response should be node 0.
    assertThat(coordinatorAddress(rs.getExecutionInfo())).isEqualTo(node0.getAddress());
    assertThat(rs.getExecutionInfo().getErrors()).isEmpty();

    // should not have been retried.
    localQuorumCounter.assertTotalCount(1);
    oneCounter.assertTotalCount(0);

    // verify log event was emitted for each host as expected
    verify(appender, after(500)).doAppend(loggingEventCaptor.capture());
    // final log message should have 2 retries
    assertThat(loggingEventCaptor.getValue().getFormattedMessage())
        .isEqualTo(
            expectedMessage(
                ConsistencyDowngradingRetryPolicy.VERDICT_ON_WRITE_TIMEOUT,
                logPrefix,
                DefaultConsistencyLevel.LOCAL_QUORUM,
                driverWriteType,
                2,
                1,
                0,
                RetryVerdict.IGNORE));
  }

  @DataProvider({"SIMPLE,SIMPLE", "BATCH,BATCH"})
  @Test
  public void should_throw_on_write_timeout_if_write_type_ignorable_but_no_ack_received(
      WriteType writeType, DefaultWriteType driverWriteType) {
    // given a node that will respond to query with a write timeout with write type that is either
    // SIMPLE or BATCH.
    node0.prime(
        when(QUERY_LOCAL_QUORUM)
            .then(writeTimeout(ConsistencyLevel.LOCAL_QUORUM, 0, 2, writeType)));

    try {
      // when executing a query.
      sessionRule.session().execute(STATEMENT_LOCAL_QUORUM);
      fail("WriteTimeoutException expected");
    } catch (WriteTimeoutException wte) {
      // then a write timeout exception is thrown
      assertThat(wte)
          .hasMessageContaining(
              "Cassandra timeout during "
                  + driverWriteType
                  + " write query at consistency LOCAL_QUORUM (2 replica were required but only 0 acknowledged the write)");
      assertThat(wte.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.LOCAL_QUORUM);
      assertThat(wte.getReceived()).isEqualTo(0);
      assertThat(wte.getBlockFor()).isEqualTo(2);
      assertThat(wte.getWriteType()).isEqualTo(driverWriteType);
      // the host that returned the response should be node 0.
      assertThat(coordinatorAddress(wte.getExecutionInfo())).isEqualTo(node0.getAddress());
      // should not have been retried
      List<Entry<Node, Throwable>> errors = wte.getExecutionInfo().getErrors();
      assertThat(errors).isEmpty();
    }

    // should not have been retried.
    localQuorumCounter.assertTotalCount(1);
    oneCounter.assertTotalCount(0);

    // verify log event was emitted for each host as expected
    verify(appender, after(500)).doAppend(loggingEventCaptor.capture());
    // final log message should have 2 retries
    assertThat(loggingEventCaptor.getValue().getFormattedMessage())
        .isEqualTo(
            expectedMessage(
                ConsistencyDowngradingRetryPolicy.VERDICT_ON_WRITE_TIMEOUT,
                logPrefix,
                DefaultConsistencyLevel.LOCAL_QUORUM,
                driverWriteType,
                2,
                0,
                0,
                RetryVerdict.RETHROW));
  }

  @Test
  public void should_downgrade_on_write_timeout_if_write_type_unlogged_batch() {
    // given a node that will respond to query with a write timeout with write type of batch log.
    node0.prime(
        when(QUERY_LOCAL_QUORUM)
            .then(writeTimeout(ConsistencyLevel.LOCAL_QUORUM, 1, 2, UNLOGGED_BATCH)));

    // when executing a query.
    ResultSet rs = sessionRule.session().execute(STATEMENT_LOCAL_QUORUM);

    // the host that returned the response should be node 0.
    assertThat(coordinatorAddress(rs.getExecutionInfo())).isEqualTo(node0.getAddress());
    // should have failed at first attempt at LOCAL_QUORUM
    List<Entry<Node, Throwable>> errors = rs.getExecutionInfo().getErrors();
    assertThat(errors).hasSize(1);
    Entry<Node, Throwable> error = errors.get(0);
    assertThat(error.getKey().getEndPoint().resolve()).isEqualTo(node0.getAddress());
    assertThat(error.getValue())
        .isInstanceOfSatisfying(
            WriteTimeoutException.class,
            wte -> {
              assertThat(wte)
                  .hasMessageContaining(
                      "Cassandra timeout during UNLOGGED_BATCH write query at consistency LOCAL_QUORUM (2 replica were required but only 1 acknowledged the write)");
              assertThat(wte.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.LOCAL_QUORUM);
              assertThat(wte.getReceived()).isEqualTo(1);
              assertThat(wte.getBlockFor()).isEqualTo(2);
              assertThat(wte.getWriteType()).isEqualTo(DefaultWriteType.UNLOGGED_BATCH);
            });

    // should have succeeded in second attempt at ONE
    Statement<?> request = (Statement<?>) rs.getExecutionInfo().getRequest();
    assertThat(request.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.ONE);

    // there should have been a retry, and it should have been executed on the same host,
    // but at consistency ONE.
    localQuorumCounter.assertTotalCount(1);
    localQuorumCounter.assertNodeCounts(1, 0, 0);
    oneCounter.assertTotalCount(1);
    oneCounter.assertNodeCounts(1, 0, 0);

    // verify 1 log event was emitted as expected
    verify(appender, timeout(500)).doAppend(loggingEventCaptor.capture());
    List<ILoggingEvent> loggedEvents = loggingEventCaptor.getAllValues();
    assertThat(loggedEvents).hasSize(1);
    assertThat(loggedEvents.get(0).getFormattedMessage())
        .isEqualTo(
            expectedMessage(
                ConsistencyDowngradingRetryPolicy.VERDICT_ON_WRITE_TIMEOUT,
                logPrefix,
                DefaultConsistencyLevel.LOCAL_QUORUM,
                DefaultWriteType.UNLOGGED_BATCH,
                2,
                1,
                0,
                new ConsistencyDowngradingRetryVerdict(DefaultConsistencyLevel.ONE)));
  }

  @Test
  public void
      should_not_downgrade_on_write_timeout_if_write_type_unlogged_batch_and_non_idempotent() {
    // given a node that will respond to query with a write timeout with write type UNLOGGED_BATCH.
    node0.prime(
        when(QUERY_LOCAL_QUORUM)
            .then(writeTimeout(ConsistencyLevel.LOCAL_QUORUM, 1, 2, UNLOGGED_BATCH)));

    try {
      // when executing a non-idempotent query.
      sessionRule.session().execute(STATEMENT_LOCAL_QUORUM.setIdempotent(false));
      fail("WriteTimeoutException expected");
    } catch (WriteTimeoutException wte) {
      // then a write timeout exception is thrown
      assertThat(wte)
          .hasMessageContaining(
              "Cassandra timeout during UNLOGGED_BATCH write query at consistency LOCAL_QUORUM (2 replica were required but only 1 acknowledged the write)");
      assertThat(wte.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.LOCAL_QUORUM);
      assertThat(wte.getReceived()).isEqualTo(1);
      assertThat(wte.getBlockFor()).isEqualTo(2);
      assertThat(wte.getWriteType()).isEqualTo(DefaultWriteType.UNLOGGED_BATCH);
      // the host that returned the response should be node 0.
      assertThat(coordinatorAddress(wte.getExecutionInfo())).isEqualTo(node0.getAddress());
      // should not have been retried
      List<Entry<Node, Throwable>> errors = wte.getExecutionInfo().getErrors();
      assertThat(errors).isEmpty();
    }

    // should not have been retried.
    localQuorumCounter.assertTotalCount(1);
    oneCounter.assertTotalCount(0);

    // expect no logging messages since there was no retry
    verify(appender, after(500).times(0)).doAppend(any(ILoggingEvent.class));
  }

  @Test
  public void should_only_retry_once_on_write_type() {
    // given a node that will respond to a query with a write timeout at 2 CLs.
    node0.prime(
        when(QUERY_LOCAL_QUORUM)
            .then(writeTimeout(ConsistencyLevel.LOCAL_QUORUM, 1, 2, UNLOGGED_BATCH)));
    node0.prime(when(QUERY_ONE).then(writeTimeout(ConsistencyLevel.ONE, 0, 1, UNLOGGED_BATCH)));
    try {
      // when executing a query.
      sessionRule.session().execute(STATEMENT_LOCAL_QUORUM);
      fail("Expected a WriteTimeoutException");
    } catch (WriteTimeoutException wte) {
      // then a write timeout exception is thrown
      assertThat(wte)
          .hasMessageContaining(
              "Cassandra timeout during UNLOGGED_BATCH write query at consistency ONE (1 replica were required but only 0 acknowledged the write)");
      assertThat(wte.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.ONE);
      assertThat(wte.getReceived()).isEqualTo(0);
      assertThat(wte.getBlockFor()).isEqualTo(1);
      assertThat(wte.getWriteType()).isEqualTo(DefaultWriteType.UNLOGGED_BATCH);
      // the host that returned the response should be node 0.
      assertThat(coordinatorAddress(wte.getExecutionInfo())).isEqualTo(node0.getAddress());
      // should have failed at first attempt at LOCAL_QUORUM as well
      List<Entry<Node, Throwable>> errors = wte.getExecutionInfo().getErrors();
      assertThat(errors).hasSize(1);
      Entry<Node, Throwable> error = errors.get(0);
      assertThat(error.getKey().getEndPoint().resolve()).isEqualTo(node0.getAddress());
      assertThat(error.getValue())
          .isInstanceOfSatisfying(
              WriteTimeoutException.class,
              wte1 -> {
                assertThat(wte1)
                    .hasMessageContaining(
                        "Cassandra timeout during UNLOGGED_BATCH write query at consistency LOCAL_QUORUM (2 replica were required but only 1 acknowledged the write)");
                assertThat(wte1.getConsistencyLevel())
                    .isEqualTo(DefaultConsistencyLevel.LOCAL_QUORUM);
                assertThat(wte1.getReceived()).isEqualTo(1);
                assertThat(wte1.getBlockFor()).isEqualTo(2);
                assertThat(wte1.getWriteType()).isEqualTo(DefaultWriteType.UNLOGGED_BATCH);
              });
    }

    // should have been retried on same host, but at consistency ONE.
    localQuorumCounter.assertTotalCount(1);
    localQuorumCounter.assertNodeCounts(1, 0, 0);
    oneCounter.assertTotalCount(1);
    oneCounter.assertNodeCounts(1, 0, 0);

    // verify log events were emitted as expected
    verify(appender, timeout(500).times(2)).doAppend(loggingEventCaptor.capture());
    List<ILoggingEvent> loggedEvents = loggingEventCaptor.getAllValues();
    assertThat(loggedEvents).hasSize(2);
    assertThat(loggedEvents.get(0).getFormattedMessage())
        .isEqualTo(
            expectedMessage(
                ConsistencyDowngradingRetryPolicy.VERDICT_ON_WRITE_TIMEOUT,
                logPrefix,
                DefaultConsistencyLevel.LOCAL_QUORUM,
                DefaultWriteType.UNLOGGED_BATCH,
                2,
                1,
                0,
                new ConsistencyDowngradingRetryVerdict(DefaultConsistencyLevel.ONE)));
    assertThat(loggedEvents.get(1).getFormattedMessage())
        .isEqualTo(
            expectedMessage(
                ConsistencyDowngradingRetryPolicy.VERDICT_ON_WRITE_TIMEOUT,
                logPrefix,
                DefaultConsistencyLevel.ONE,
                DefaultWriteType.UNLOGGED_BATCH,
                1,
                0,
                1,
                RetryVerdict.RETHROW));
  }

  @Test
  public void should_retry_on_next_host_on_unavailable_if_LWT() {
    // given a node that will respond to a query with an unavailable.
    node0.prime(when(QUERY_LOCAL_SERIAL).then(unavailable(ConsistencyLevel.LOCAL_SERIAL, 2, 1)));

    // when executing a query.
    ResultSet result = sessionRule.session().execute(STATEMENT_LOCAL_SERIAL);
    // then we should get a response, and the host that returned the response should be node 1.
    assertThat(coordinatorAddress(result.getExecutionInfo())).isEqualTo(node1.getAddress());
    // the execution info on the result set indicates there was
    // an error on the host that received the query.
    assertThat(result.getExecutionInfo().getErrors()).hasSize(1);
    Map.Entry<Node, Throwable> error = result.getExecutionInfo().getErrors().get(0);
    assertThat(error.getKey().getEndPoint().resolve()).isEqualTo(node0.getAddress());
    assertThat(error.getValue())
        .isInstanceOfSatisfying(
            UnavailableException.class,
            ue -> {
              assertThat(ue)
                  .hasMessageContaining(
                      "Not enough replicas available for query at consistency LOCAL_SERIAL (2 required but only 1 alive)");
              assertThat(ue.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.LOCAL_SERIAL);
              assertThat(ue.getAlive()).isEqualTo(1);
              assertThat(ue.getRequired()).isEqualTo(2);
            });

    // should have been retried on another host.
    localSerialCounter.assertTotalCount(2);
    localSerialCounter.assertNodeCounts(1, 1, 0);
    localQuorumCounter.assertTotalCount(0);
    oneCounter.assertTotalCount(0);

    // verify log event was emitted as expected
    verify(appender, timeout(500)).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getValue().getFormattedMessage())
        .isEqualTo(
            expectedMessage(
                ConsistencyDowngradingRetryPolicy.VERDICT_ON_UNAVAILABLE,
                logPrefix,
                DefaultConsistencyLevel.LOCAL_SERIAL,
                2,
                1,
                0,
                RetryVerdict.RETRY_NEXT));
  }

  @Test
  public void should_downgrade_on_unavailable() {
    // given a node that will respond to a query with an unavailable.
    node0.prime(when(QUERY_LOCAL_QUORUM).then(unavailable(ConsistencyLevel.LOCAL_QUORUM, 2, 1)));

    // when executing a query.
    ResultSet rs = sessionRule.session().execute(STATEMENT_LOCAL_QUORUM);
    // then we should get a response, and the host that returned the response should be node 0.
    assertThat(coordinatorAddress(rs.getExecutionInfo())).isEqualTo(node0.getAddress());
    // the execution info on the result set indicates there was
    // an error on the host that received the query.
    assertThat(rs.getExecutionInfo().getErrors()).hasSize(1);
    Map.Entry<Node, Throwable> error = rs.getExecutionInfo().getErrors().get(0);
    assertThat(error.getKey().getEndPoint().resolve()).isEqualTo(node0.getAddress());
    assertThat(error.getValue())
        .isInstanceOfSatisfying(
            UnavailableException.class,
            ue -> {
              assertThat(ue)
                  .hasMessageContaining(
                      "Not enough replicas available for query at consistency LOCAL_QUORUM (2 required but only 1 alive)");
              assertThat(ue.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.LOCAL_QUORUM);
              assertThat(ue.getAlive()).isEqualTo(1);
              assertThat(ue.getRequired()).isEqualTo(2);
            });

    // should have succeeded in second attempt at ONE
    Statement<?> request = (Statement<?>) rs.getExecutionInfo().getRequest();
    assertThat(request.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.ONE);

    // should have been retried on the same host, but at ONE.
    localQuorumCounter.assertTotalCount(1);
    localQuorumCounter.assertNodeCounts(1, 0, 0);
    oneCounter.assertTotalCount(1);
    oneCounter.assertNodeCounts(1, 0, 0);

    // verify log event was emitted as expected
    verify(appender, timeout(500)).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getValue().getFormattedMessage())
        .isEqualTo(
            expectedMessage(
                ConsistencyDowngradingRetryPolicy.VERDICT_ON_UNAVAILABLE,
                logPrefix,
                DefaultConsistencyLevel.LOCAL_QUORUM,
                2,
                1,
                0,
                new ConsistencyDowngradingRetryVerdict(DefaultConsistencyLevel.ONE)));
  }

  @Test
  public void should_only_retry_once_on_unavailable() {
    // given two nodes that will respond to a query with an unavailable.
    node0.prime(when(QUERY_LOCAL_QUORUM).then(unavailable(ConsistencyLevel.LOCAL_QUORUM, 2, 1)));
    node0.prime(when(QUERY_ONE).then(unavailable(ConsistencyLevel.ONE, 1, 0)));

    try {
      // when executing a query.
      sessionRule.session().execute(STATEMENT_LOCAL_QUORUM);
      fail("Expected an UnavailableException");
    } catch (UnavailableException ue) {
      // then we should get an unavailable exception with the host being node 1 (since it was second
      // tried).
      assertThat(ue)
          .hasMessageContaining(
              "Not enough replicas available for query at consistency ONE (1 required but only 0 alive)");
      assertThat(ue.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.ONE);
      assertThat(ue.getRequired()).isEqualTo(1);
      assertThat(ue.getAlive()).isEqualTo(0);
      assertThat(ue.getExecutionInfo().getErrors()).hasSize(1);
      Map.Entry<Node, Throwable> error = ue.getExecutionInfo().getErrors().get(0);
      assertThat(error.getKey().getEndPoint().resolve()).isEqualTo(node0.getAddress());
      assertThat(error.getValue())
          .isInstanceOfSatisfying(
              UnavailableException.class,
              ue1 -> {
                assertThat(ue1)
                    .hasMessageContaining(
                        "Not enough replicas available for query at consistency LOCAL_QUORUM (2 required but only 1 alive)");
                assertThat(ue1.getConsistencyLevel())
                    .isEqualTo(DefaultConsistencyLevel.LOCAL_QUORUM);
                assertThat(ue1.getRequired()).isEqualTo(2);
                assertThat(ue1.getAlive()).isEqualTo(1);
              });
    }

    // should have been retried on same host, but at ONE.
    localQuorumCounter.assertTotalCount(1);
    localQuorumCounter.assertNodeCounts(1, 0, 0);
    oneCounter.assertTotalCount(1);
    oneCounter.assertNodeCounts(1, 0, 0);

    // verify log events were emitted as expected
    verify(appender, timeout(500).times(2)).doAppend(loggingEventCaptor.capture());
    List<ILoggingEvent> loggedEvents = loggingEventCaptor.getAllValues();
    assertThat(loggedEvents).hasSize(2);
    assertThat(loggedEvents.get(0).getFormattedMessage())
        .isEqualTo(
            expectedMessage(
                ConsistencyDowngradingRetryPolicy.VERDICT_ON_UNAVAILABLE,
                logPrefix,
                DefaultConsistencyLevel.LOCAL_QUORUM,
                2,
                1,
                0,
                new ConsistencyDowngradingRetryVerdict(DefaultConsistencyLevel.ONE)));
    assertThat(loggedEvents.get(1).getFormattedMessage())
        .isEqualTo(
            expectedMessage(
                ConsistencyDowngradingRetryPolicy.VERDICT_ON_UNAVAILABLE,
                logPrefix,
                DefaultConsistencyLevel.ONE,
                1,
                0,
                1,
                RetryVerdict.RETHROW));
  }

  @Test
  public void should_retry_on_next_host_on_connection_error_if_idempotent() {
    // given a node that will close its connection as result of receiving a query.
    node0.prime(
        when(QUERY_LOCAL_QUORUM)
            .then(closeConnection(DisconnectAction.Scope.CONNECTION, CloseType.DISCONNECT)));

    // when executing a query.
    ResultSet result = sessionRule.session().execute(STATEMENT_LOCAL_QUORUM);
    // then we should get a response, and the execution info on the result set indicates there was
    // an error on the host that received the query.
    assertThat(result.getExecutionInfo().getErrors()).hasSize(1);
    Map.Entry<Node, Throwable> error = result.getExecutionInfo().getErrors().get(0);
    assertThat(error.getKey().getEndPoint().resolve()).isEqualTo(node0.getAddress());
    assertThat(error.getValue()).isInstanceOf(ClosedConnectionException.class);
    // the host that returned the response should be node 1.
    assertThat(coordinatorAddress(result.getExecutionInfo())).isEqualTo(node1.getAddress());

    // should have been retried.
    localQuorumCounter.assertTotalCount(2);
    // expected query on node 0, and retry on node 2.
    localQuorumCounter.assertNodeCounts(1, 1, 0);
    oneCounter.assertTotalCount(0);

    // verify log event was emitted as expected
    verify(appender, timeout(500)).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getValue().getFormattedMessage())
        .isEqualTo(
            expectedMessage(
                ConsistencyDowngradingRetryPolicy.VERDICT_ON_ABORTED,
                logPrefix,
                ClosedConnectionException.class.getSimpleName(),
                error.getValue().getMessage(),
                0,
                RetryVerdict.RETRY_NEXT));
  }

  @Test
  public void should_keep_retrying_on_next_host_on_connection_error() {
    // given a request for which every node will close its connection upon receiving it.
    SIMULACRON_RULE
        .cluster()
        .prime(
            when(QUERY_LOCAL_QUORUM)
                .then(closeConnection(DisconnectAction.Scope.CONNECTION, CloseType.DISCONNECT)));

    try {
      // when executing a query.
      sessionRule.session().execute(STATEMENT_LOCAL_QUORUM);
      fail("AllNodesFailedException expected");
    } catch (AllNodesFailedException ex) {
      // then an AllNodesFailedException should be raised indicating that all nodes failed the
      // request.
      assertThat(ex.getAllErrors()).hasSize(3);
    }

    // should have been tried on all nodes.
    // should have been retried.
    localQuorumCounter.assertTotalCount(3);
    // expected query on node 0, and retry on node 2 and 3.
    localQuorumCounter.assertNodeCounts(1, 1, 1);
    oneCounter.assertTotalCount(0);

    // verify log event was emitted for each host as expected
    verify(appender, after(500).times(3)).doAppend(loggingEventCaptor.capture());
    // final log message should have 2 retries
    assertThat(loggingEventCaptor.getValue().getFormattedMessage())
        .isEqualTo(
            expectedMessage(
                ConsistencyDowngradingRetryPolicy.VERDICT_ON_ABORTED,
                logPrefix,
                ClosedConnectionException.class.getSimpleName(),
                "Lost connection to remote peer",
                2,
                RetryVerdict.RETRY_NEXT));
  }

  @Test
  public void should_not_retry_on_connection_error_if_non_idempotent() {
    // given a node that will close its connection as result of receiving a query.
    node0.prime(
        when(QUERY_LOCAL_QUORUM)
            .then(closeConnection(DisconnectAction.Scope.CONNECTION, CloseType.DISCONNECT)));

    try {
      // when executing a non-idempotent query.
      sessionRule.session().execute(STATEMENT_LOCAL_QUORUM.setIdempotent(false));
      fail("ClosedConnectionException expected");
    } catch (ClosedConnectionException ex) {
      // then a ClosedConnectionException should be raised, indicating that the connection closed
      // while handling the request on that node.
      // this clearly indicates that the request wasn't retried.
      // Exception should indicate that node 0 was the failing node.
      // FIXME JAVA-2908
      //      Node coordinator = ex.getExecutionInfo().getCoordinator();
      //      assertThat(coordinator).isNotNull();
      //      assertThat(coordinator.getEndPoint().resolve())
      //          .isEqualTo(SIMULACRON_RULE.cluster().node(0).getAddress());
    }

    // should not have been retried.
    localQuorumCounter.assertTotalCount(1);
    oneCounter.assertTotalCount(0);

    // expect no logging messages since there was no retry
    verify(appender, after(500).times(0)).doAppend(any(ILoggingEvent.class));
  }

  @Test
  public void should_keep_retrying_on_next_host_on_error_response() {
    // given every node responding with a server error.
    SIMULACRON_RULE
        .cluster()
        .prime(when(QUERY_LOCAL_QUORUM).then(serverError("this is a server error")));

    try {
      // when executing a query.
      sessionRule.session().execute(STATEMENT_LOCAL_QUORUM);
      fail("Expected an AllNodesFailedException");
    } catch (AllNodesFailedException e) {
      // then we should get an all nodes failed exception, indicating the query was tried each node.
      assertThat(e.getAllErrors()).hasSize(3);
      for (List<Throwable> nodeErrors : e.getAllErrors().values()) {
        for (Throwable nodeError : nodeErrors) {
          assertThat(nodeError).isInstanceOf(ServerError.class);
          assertThat(nodeError).hasMessage("this is a server error");
        }
      }
    }

    // should have been tried on all nodes.
    localQuorumCounter.assertTotalCount(3);
    localQuorumCounter.assertNodeCounts(1, 1, 1);

    // verify log event was emitted for each host as expected
    verify(appender, after(500).times(3)).doAppend(loggingEventCaptor.capture());
    // final log message should have 2 retries
    assertThat(loggingEventCaptor.getValue().getFormattedMessage())
        .isEqualTo(
            expectedMessage(
                ConsistencyDowngradingRetryPolicy.VERDICT_ON_ERROR,
                logPrefix,
                ServerError.class.getSimpleName(),
                "this is a server error",
                2,
                RetryVerdict.RETRY_NEXT));
  }

  @Test
  public void should_not_retry_on_next_host_on_error_response_if_write_failure() {
    // given every node responding with a write failure.
    SIMULACRON_RULE
        .cluster()
        .prime(
            when(QUERY_LOCAL_QUORUM)
                .then(
                    writeFailure(
                        ConsistencyLevel.LOCAL_QUORUM, 1, 2, ImmutableMap.of(), WriteType.SIMPLE)));
    try {
      // when executing a query
      sessionRule.session().execute(STATEMENT_LOCAL_QUORUM);
      fail("Expected a WriteFailureException");
    } catch (WriteFailureException wfe) {
      // then we should get a write failure exception with the host being node 1 (since it was
      // second tried).
      assertThat(wfe)
          .hasMessageContaining(
              "Cassandra failure during write query at consistency LOCAL_QUORUM (2 responses were required but only 1 replica responded, 0 failed)");
      assertThat(wfe.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.LOCAL_QUORUM);
      assertThat(wfe.getBlockFor()).isEqualTo(2);
      assertThat(wfe.getReceived()).isEqualTo(1);
      assertThat(wfe.getWriteType()).isEqualTo(DefaultWriteType.SIMPLE);
      assertThat(wfe.getReasonMap()).isEmpty();
    }

    // should only have been tried on first node.
    localQuorumCounter.assertTotalCount(1);
    localQuorumCounter.assertNodeCounts(1, 0, 0);
    oneCounter.assertTotalCount(0);

    // verify log event was emitted as expected
    verify(appender, timeout(500)).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getValue().getFormattedMessage())
        .isEqualTo(
            expectedMessage(
                ConsistencyDowngradingRetryPolicy.VERDICT_ON_ERROR,
                logPrefix,
                WriteFailureException.class.getSimpleName(),
                "Cassandra failure during write query at consistency LOCAL_QUORUM (2 responses were required but only 1 replica responded, 0 failed)",
                0,
                RetryVerdict.RETHROW));
  }

  @Test
  public void should_not_retry_on_next_host_on_error_response_if_read_failure() {
    // given every node responding with a read failure.
    SIMULACRON_RULE
        .cluster()
        .prime(
            when(QUERY_LOCAL_QUORUM)
                .then(readFailure(ConsistencyLevel.LOCAL_QUORUM, 1, 2, ImmutableMap.of(), true)));
    try {
      // when executing a query
      sessionRule.session().execute(STATEMENT_LOCAL_QUORUM);
      fail("Expected a ReadFailureException");
    } catch (ReadFailureException rfe) {
      // then we should get a read failure exception with the host being node 1 (since it was
      // second tried).
      assertThat(rfe)
          .hasMessageContaining(
              "Cassandra failure during read query at consistency LOCAL_QUORUM (2 responses were required but only 1 replica responded, 0 failed)");
      assertThat(rfe.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.LOCAL_QUORUM);
      assertThat(rfe.getBlockFor()).isEqualTo(2);
      assertThat(rfe.getReceived()).isEqualTo(1);
      assertThat(rfe.wasDataPresent()).isTrue();
      assertThat(rfe.getReasonMap()).isEmpty();
    }

    // should only have been tried on first node.
    localQuorumCounter.assertTotalCount(1);
    localQuorumCounter.assertNodeCounts(1, 0, 0);
    oneCounter.assertTotalCount(0);

    // verify log event was emitted as expected
    verify(appender, timeout(500)).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getValue().getFormattedMessage())
        .isEqualTo(
            expectedMessage(
                ConsistencyDowngradingRetryPolicy.VERDICT_ON_ERROR,
                logPrefix,
                ReadFailureException.class.getSimpleName(),
                "Cassandra failure during read query at consistency LOCAL_QUORUM (2 responses were required but only 1 replica responded, 0 failed)",
                0,
                RetryVerdict.RETHROW));
  }

  @Test
  public void should_not_retry_on_next_host_on_error_response_if_non_idempotent() {
    // given every node responding with a server error.
    SIMULACRON_RULE
        .cluster()
        .prime(when(QUERY_LOCAL_QUORUM).then(serverError("this is a server error")));

    try {
      // when executing a query that is not idempotent
      sessionRule.session().execute(STATEMENT_LOCAL_QUORUM.setIdempotent(false));
      fail("Expected a ServerError");
    } catch (ServerError e) {
      // then should get a server error from first host.
      assertThat(e.getMessage()).isEqualTo("this is a server error");
    }

    // should only have been tried on first node.
    localQuorumCounter.assertTotalCount(1);
    localQuorumCounter.assertNodeCounts(1, 0, 0);
    oneCounter.assertTotalCount(0);

    // expect no logging messages since there was no retry
    verify(appender, after(500).times(0)).doAppend(any(ILoggingEvent.class));
  }

  private String expectedMessage(String template, Object... args) {
    return MessageFormatter.arrayFormat(template, args).getMessage();
  }

  private SocketAddress coordinatorAddress(ExecutionInfo executionInfo) {
    Node coordinator = executionInfo.getCoordinator();
    assertThat(coordinator).isNotNull();
    return coordinator.getEndPoint().resolve();
  }
}
