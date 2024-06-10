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

/*
 * Copyright (C) 2022 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.internal.core.util.concurrent;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.rows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Fail.fail;

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import java.util.UUID;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

/**
 * This test exercises the driver with BlockHound installed and tests that the rules defined in
 * {@link DriverBlockHoundIntegration} are being applied.
 */
@Category(IsolatedTests.class)
public class DriverBlockHoundIntegrationIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(DriverBlockHoundIntegrationIT.class);

  @ClassRule
  public static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(1));

  @BeforeClass
  public static void setUp() {
    try {
      BlockHound.install();
    } catch (Throwable t) {
      LOGGER.error("BlockHound could not be installed", t);
      fail("BlockHound could not be installed", t);
    }
  }

  @Before
  public void setup() {
    SIMULACRON_RULE.cluster().prime(when("SELECT c1, c2 FROM ks.t1").then(rows().row("foo", 42)));
  }

  @Test
  @SuppressWarnings("BlockingMethodInNonBlockingContext")
  public void should_detect_blocking_call() {
    // this is just to make sure the detection mechanism is properly installed
    Mono<Integer> blockingPublisher =
        Mono.fromCallable(
                () -> {
                  Thread.sleep(1);
                  return 0;
                })
            .subscribeOn(Schedulers.parallel());
    StepVerifier.create(blockingPublisher)
        .expectErrorMatches(e -> e instanceof Error && e.getMessage().contains("Blocking call!"))
        .verify();
  }

  @Test
  public void should_not_detect_blocking_call_on_asynchronous_execution() {
    try (CqlSession session = SessionUtils.newSession(SIMULACRON_RULE)) {
      Flux<ReactiveRow> rows =
          Flux.range(0, 1000)
              .flatMap(
                  i ->
                      Flux.from(session.executeReactive("SELECT c1, c2 FROM ks.t1"))
                          .subscribeOn(Schedulers.parallel()));
      StepVerifier.create(rows).expectNextCount(1000).expectComplete().verify();
    }
  }

  @Test
  @Ignore("@IntegrationTestDisabledCassandra3Failure")
  public void should_not_detect_blocking_call_on_asynchronous_execution_prepared() {
    try (CqlSession session = SessionUtils.newSession(SIMULACRON_RULE)) {
      Flux<ReactiveRow> rows =
          Mono.fromCompletionStage(() -> session.prepareAsync("SELECT c1, c2 FROM ks.t1"))
              .flatMapMany(
                  ps ->
                      Flux.range(0, 1000)
                          .map(i -> ps.bind())
                          .flatMap(
                              bs ->
                                  Flux.from(session.executeReactive(bs))
                                      .subscribeOn(Schedulers.parallel())));
      StepVerifier.create(rows).expectNextCount(1000).expectComplete().verify();
    }
  }

  @Test
  public void should_not_detect_blocking_call_on_random_uuid_generation() {
    Flux<UUID> uuids =
        Flux.<UUID>create(
                sink -> {
                  for (int i = 0; i < 1_000_000; ++i) {
                    sink.next(Uuids.random());
                  }
                  sink.complete();
                })
            .subscribeOn(Schedulers.parallel());
    StepVerifier.create(uuids).expectNextCount(1_000_000).expectComplete().verify();
  }

  @Test
  public void should_not_detect_blocking_call_on_time_based_uuid_generation() {
    Flux<UUID> uuids =
        Flux.<UUID>create(
                sink -> {
                  for (int i = 0; i < 1_000_000; ++i) {
                    sink.next(Uuids.timeBased());
                  }
                  sink.complete();
                })
            .subscribeOn(Schedulers.parallel());
    StepVerifier.create(uuids).expectNextCount(1_000_000).expectComplete().verify();
  }
}
