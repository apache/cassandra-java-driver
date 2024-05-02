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
package com.datastax.oss.driver.internal.core.util.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.ThreadFactoryBuilder;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Test;

public class CycleDetectorTest {

  @Test
  public void should_detect_cycle_within_same_thread() {
    CycleDetector checker = new CycleDetector("Detected cycle", true);
    CyclicContext context = new CyclicContext(checker, false);
    try {
      context.a.get();
      fail("Expected an exception");
    } catch (Exception e) {
      assertThat(e)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Detected cycle");
    }
  }

  @Test
  public void should_detect_cycle_between_different_threads() throws Throwable {
    CycleDetector checker = new CycleDetector("Detected cycle", true);
    CyclicContext context = new CyclicContext(checker, true);
    ExecutorService executor =
        Executors.newFixedThreadPool(
            3, new ThreadFactoryBuilder().setNameFormat("thread%d").build());
    Future<String> futureA = executor.submit(() -> context.a.get());
    Future<String> futureB = executor.submit(() -> context.b.get());
    Future<String> futureC = executor.submit(() -> context.c.get());
    context.latchA.countDown();
    context.latchB.countDown();
    context.latchC.countDown();
    for (Future<String> future : ImmutableList.of(futureA, futureB, futureC)) {
      try {
        Uninterruptibles.getUninterruptibly(future);
      } catch (ExecutionException e) {
        assertThat(e.getCause())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Detected cycle");
      }
    }
  }

  private static class CyclicContext {
    private LazyReference<String> a;
    private LazyReference<String> b;
    private LazyReference<String> c;
    private CountDownLatch latchA;
    private CountDownLatch latchB;
    private CountDownLatch latchC;

    private CyclicContext(CycleDetector checker, boolean enableLatches) {
      this.a = new LazyReference<>("a", this::buildA, checker);
      this.b = new LazyReference<>("b", this::buildB, checker);
      this.c = new LazyReference<>("c", this::buildC, checker);
      if (enableLatches) {
        this.latchA = new CountDownLatch(1);
        this.latchB = new CountDownLatch(1);
        this.latchC = new CountDownLatch(1);
      }
    }

    private String buildA() {
      maybeAwaitUninterruptibly(latchA);
      b.get();
      return "a";
    }

    private String buildB() {
      maybeAwaitUninterruptibly(latchB);
      c.get();
      return "b";
    }

    private String buildC() {
      maybeAwaitUninterruptibly(latchC);
      a.get();
      return "c";
    }

    private static void maybeAwaitUninterruptibly(CountDownLatch latch) {
      if (latch != null) {
        try {
          latch.await();
        } catch (InterruptedException e) {
          fail("interrupted", e);
        }
      }
    }
  }
}
