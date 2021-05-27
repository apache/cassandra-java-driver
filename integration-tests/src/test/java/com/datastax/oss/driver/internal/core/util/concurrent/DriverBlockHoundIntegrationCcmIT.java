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
package com.datastax.oss.driver.internal.core.util.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.cql.continuous.ContinuousAsyncResultSet;
import com.datastax.dse.driver.api.core.cql.continuous.ContinuousPagingITBase;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.IsolatedTests;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

/**
 * This test exercises the driver with BlockHound installed and tests that the rules defined in
 * {@link DriverBlockHoundIntegration} are being applied, and especially when continuous paging is
 * used.
 */
@DseRequirement(
    min = "5.1.0",
    description = "Continuous paging is only available from 5.1.0 onwards")
@Category(IsolatedTests.class)
public class DriverBlockHoundIntegrationCcmIT extends ContinuousPagingITBase {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DriverBlockHoundIntegrationCcmIT.class);

  private static final CustomCcmRule CCM_RULE = CustomCcmRule.builder().build();

  // Note: Insights monitoring will be detected by BlockHound, but the error is swallowed and
  // logged by DefaultSession.SingleThreaded.notifyListeners, so it's not necessary to explicitly
  // disable Insights here.
  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  @BeforeClass
  public static void setUp() {
    try {
      BlockHound.install();
    } catch (Throwable t) {
      LOGGER.error("BlockHound could not be installed", t);
      fail("BlockHound could not be installed", t);
    }
    initialize(SESSION_RULE.session(), SESSION_RULE.slowProfile());
  }

  @Test
  public void should_not_detect_blocking_call_with_continuous_paging() {
    CqlSession session = SESSION_RULE.session();
    SimpleStatement statement = SimpleStatement.newInstance("SELECT v from test where k=?", KEY);
    Flux<Row> rows =
        Flux.range(0, 10)
            .flatMap(
                i ->
                    Flux.fromIterable(session.executeContinuously(statement))
                        .subscribeOn(Schedulers.parallel()));
    StepVerifier.create(rows).expectNextCount(1000).expectComplete().verify();
  }

  /** Copied from com.datastax.dse.driver.api.core.cql.continuous.ContinuousPagingIT. */
  @Test
  public void should_not_detect_blocking_call_with_continuous_paging_when_timeout()
      throws Exception {
    CqlSession session = SESSION_RULE.session();
    SimpleStatement statement = SimpleStatement.newInstance("SELECT v from test where k=?", KEY);
    // Throttle server at a page per second and set client timeout much lower so that the client
    // will experience a timeout.
    // Note that this might not be perfect if there are pauses in the JVM and the timeout
    // doesn't fire soon enough.
    DriverExecutionProfile profile =
        session
            .getContext()
            .getConfig()
            .getDefaultProfile()
            .withInt(DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE, 10)
            .withInt(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND, 1)
            .withDuration(
                DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_OTHER_PAGES, Duration.ofMillis(100));
    CompletionStage<ContinuousAsyncResultSet> future =
        session.executeContinuouslyAsync(statement.setExecutionProfile(profile));
    ContinuousAsyncResultSet pagingResult = CompletableFutures.getUninterruptibly(future);
    try {
      pagingResult.fetchNextPage().toCompletableFuture().get();
      fail("Expected a timeout");
    } catch (ExecutionException e) {
      assertThat(e.getCause())
          .isInstanceOf(DriverTimeoutException.class)
          .hasMessageContaining("Timed out waiting for page 2");
    }
  }
}
