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

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.io.IOException;
import org.junit.Test;

public class PromiseCombinerTest {

  private final EventExecutor executor = ImmediateEventExecutor.INSTANCE;

  @Test
  public void should_complete_normally_if_all_parents_complete_normally() {
    // given
    Promise<Void> promise = executor.newPromise();
    Promise<Void> parent1 = executor.newPromise();
    Promise<Void> parent2 = executor.newPromise();
    // when
    PromiseCombiner.combine(promise, parent1, parent2);
    parent1.setSuccess(null);
    parent2.setSuccess(null);
    // then
    assertThat(promise.isSuccess()).isTrue();
  }

  @Test
  public void should_complete_exceptionally_if_any_parent_completes_exceptionally() {
    // given
    Promise<Void> promise = executor.newPromise();
    Promise<Void> parent1 = executor.newPromise();
    Promise<Void> parent2 = executor.newPromise();
    Promise<Void> parent3 = executor.newPromise();
    NullPointerException npe = new NullPointerException();
    IOException ioe = new IOException();
    // when
    PromiseCombiner.combine(promise, parent1, parent2, parent3);
    parent1.setSuccess(null);
    parent2.setFailure(npe);
    parent3.setFailure(ioe);
    // then
    assertThat(promise.isSuccess()).isFalse();
    assertThat(promise.cause()).isSameAs(npe).hasSuppressedException(ioe);
  }
}
