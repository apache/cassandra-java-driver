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
package com.datastax.oss.driver.internal.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import io.netty.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.assertj.core.api.AbstractAssert;

public class NettyFutureAssert<V> extends AbstractAssert<NettyFutureAssert<V>, Future<V>> {

  public NettyFutureAssert(Future<V> actual) {
    super(actual, NettyFutureAssert.class);
  }

  public NettyFutureAssert isNotDone() {
    assertThat(actual.isDone()).isFalse();
    return this;
  }

  public NettyFutureAssert<V> isSuccess(Consumer<V> valueAssertions) {
    try {
      V value = actual.get(100, TimeUnit.MILLISECONDS);
      valueAssertions.accept(value);
    } catch (TimeoutException e) {
      fail("Future did not complete within the timeout");
    } catch (Throwable t) {
      fail("Unexpected error while waiting on the future", t);
    }
    return this;
  }

  public NettyFutureAssert<V> isSuccess() {
    return isSuccess(v -> {});
  }

  public NettyFutureAssert isFailed(Consumer<Throwable> failureAssertions) {
    try {
      actual.get(100, TimeUnit.MILLISECONDS);
      fail("Expected future to fail");
    } catch (TimeoutException e) {
      fail("Future did not fail within the timeout");
    } catch (InterruptedException e) {
      fail("Interrupted while waiting for future to fail");
    } catch (ExecutionException e) {
      failureAssertions.accept(e.getCause());
    }
    return this;
  }

  public NettyFutureAssert isFailed() {
    return isFailed(f -> {});
  }
}
