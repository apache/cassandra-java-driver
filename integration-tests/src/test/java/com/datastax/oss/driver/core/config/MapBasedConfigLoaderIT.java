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
package com.datastax.oss.driver.core.config;

import static com.datastax.oss.simulacron.common.codec.ConsistencyLevel.QUORUM;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.unavailable;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.config.ConfigChangeEvent;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class MapBasedConfigLoaderIT {

  @ClassRule
  public static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(1));

  @Before
  public void setup() {
    SIMULACRON_RULE.cluster().clearPrimes(true);
  }

  /**
   * Checks that runtime changes to the pool size are reflected in the driver. This is a special
   * case because unlike other options, the driver does not re-read the option at regular intervals;
   * instead, it relies on the {@link ConfigChangeEvent} being fired.
   */
  @Test
  public void should_resize_pool_when_config_changes() {
    OptionsMap optionsMap = OptionsMap.driverDefaults();

    try (CqlSession session =
        CqlSession.builder()
            .addContactEndPoints(SIMULACRON_RULE.getContactPoints())
            .withLocalDatacenter("dc1")
            .withConfigLoader(DriverConfigLoader.fromMap(optionsMap))
            .build()) {

      Node node = session.getMetadata().getNodes().values().iterator().next();
      assertThat(node.getOpenConnections()).isEqualTo(2); // control connection + pool (default 1)

      optionsMap.put(TypedDriverOption.CONNECTION_POOL_LOCAL_SIZE, 2);

      await()
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .atMost(60, TimeUnit.SECONDS)
          .until(() -> node.getOpenConnections() == 3);
    }
  }

  /** Checks that profiles that have specific policy options will get their own policy instance. */
  @Test
  public void should_create_policies_per_profile() {
    // Given
    // a query that throws UNAVAILABLE
    String mockQuery = "mock query";
    SIMULACRON_RULE.cluster().prime(when(mockQuery).then(unavailable(QUORUM, 3, 2)));

    // a default profile that uses the default retry policy, and an alternate profile that uses a
    // policy that ignores all errors
    OptionsMap optionsMap = OptionsMap.driverDefaults();
    String alternateProfile = "profile1";
    optionsMap.put(
        alternateProfile, TypedDriverOption.RETRY_POLICY_CLASS, IgnoreAllPolicy.class.getName());

    try (CqlSession session =
        CqlSession.builder()
            .addContactEndPoints(SIMULACRON_RULE.getContactPoints())
            .withLocalDatacenter("dc1")
            .withConfigLoader(DriverConfigLoader.fromMap(optionsMap))
            .build()) {

      // When
      // executing the query for the default profile
      SimpleStatement defaultProfileStatement = SimpleStatement.newInstance(mockQuery);
      assertThatThrownBy(() -> session.execute(defaultProfileStatement))
          .satisfies(
              t -> {
                // Then
                // the UNAVAILABLE error is surfaced
                assertThat(t).isInstanceOf(AllNodesFailedException.class);
                AllNodesFailedException anfe = (AllNodesFailedException) t;
                assertThat(anfe.getAllErrors()).hasSize(1);
                List<Throwable> nodeErrors = anfe.getAllErrors().values().iterator().next();
                assertThat(nodeErrors).hasSize(1);
                assertThat(nodeErrors.get(0)).isInstanceOf(UnavailableException.class);
              });

      // When
      // executing the query for the alternate profile
      SimpleStatement alternateProfileStatement =
          SimpleStatement.newInstance(mockQuery).setExecutionProfileName(alternateProfile);
      ResultSet rs = session.execute(alternateProfileStatement);

      // Then
      // the error is ignored
      assertThat(rs.one()).isNull();
    }
  }

  public static class IgnoreAllPolicy implements RetryPolicy {

    public IgnoreAllPolicy(
        @SuppressWarnings("unused") DriverContext context,
        @SuppressWarnings("unused") String profile) {
      // nothing to do
    }

    @Override
    @Deprecated
    public RetryDecision onReadTimeout(
        @NonNull Request request,
        @NonNull ConsistencyLevel cl,
        int blockFor,
        int received,
        boolean dataPresent,
        int retryCount) {
      return RetryDecision.IGNORE;
    }

    @Override
    @Deprecated
    public RetryDecision onWriteTimeout(
        @NonNull Request request,
        @NonNull ConsistencyLevel cl,
        @NonNull WriteType writeType,
        int blockFor,
        int received,
        int retryCount) {
      return RetryDecision.IGNORE;
    }

    @Override
    @Deprecated
    public RetryDecision onUnavailable(
        @NonNull Request request,
        @NonNull ConsistencyLevel cl,
        int required,
        int alive,
        int retryCount) {
      return RetryDecision.IGNORE;
    }

    @Override
    @Deprecated
    public RetryDecision onRequestAborted(
        @NonNull Request request, @NonNull Throwable error, int retryCount) {
      return RetryDecision.IGNORE;
    }

    @Override
    @Deprecated
    public RetryDecision onErrorResponse(
        @NonNull Request request, @NonNull CoordinatorException error, int retryCount) {
      return RetryDecision.IGNORE;
    }

    @Override
    public void close() {
      // nothing to do
    }
  }
}
