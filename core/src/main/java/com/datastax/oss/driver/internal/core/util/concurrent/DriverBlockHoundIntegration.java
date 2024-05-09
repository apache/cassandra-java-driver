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

import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation.InternalThread;
import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

public final class DriverBlockHoundIntegration implements BlockHoundIntegration {

  @Override
  public void applyTo(BlockHound.Builder builder) {

    // disallow blocking operations in driver internal threads by default;
    // note that session initialization will happen on one of these threads, which is why
    // we need to allow a few blocking calls below.
    builder.nonBlockingThreadPredicate(current -> current.or(InternalThread.class::isInstance));

    // blocking calls in initialization methods

    builder.allowBlockingCallsInside(
        "com.datastax.oss.driver.internal.core.context.DefaultNettyOptions", "createTimer");
    builder.allowBlockingCallsInside(
        "com.datastax.oss.driver.internal.core.os.Native$LibcLoader", "load");
    builder.allowBlockingCallsInside(
        // requires native libraries
        "com.datastax.oss.driver.internal.core.time.Clock", "getInstance");
    builder.allowBlockingCallsInside(
        "com.datastax.oss.driver.internal.core.util.concurrent.LazyReference", "get");
    builder.allowBlockingCallsInside(
        "com.datastax.oss.driver.internal.core.util.concurrent.ReplayingEventFilter", "accept");
    builder.allowBlockingCallsInside(
        "com.datastax.oss.driver.internal.core.util.concurrent.ReplayingEventFilter", "markReady");
    builder.allowBlockingCallsInside(
        "com.datastax.oss.driver.internal.core.util.concurrent.ReplayingEventFilter", "start");

    // called upon initialization but also on topology/status events

    builder.allowBlockingCallsInside(
        "com.datastax.oss.driver.internal.core.metadata.LoadBalancingPolicyWrapper$SinglePolicyDistanceReporter",
        "setDistance");
    builder.allowBlockingCallsInside(
        "com.datastax.oss.driver.internal.core.pool.ChannelSet", "add");
    builder.allowBlockingCallsInside(
        "com.datastax.oss.driver.internal.core.pool.ChannelSet", "remove");

    // never called directly by the driver; locks that usually operate with low thread contention

    builder.allowBlockingCallsInside(
        "com.datastax.oss.driver.internal.core.type.codec.registry.CachingCodecRegistry",
        "register");
    builder.allowBlockingCallsInside(
        // requires native libraries, for now because of Uuids.getProcessPiece; if JAVA-1116 gets
        // implemented, Uuids.getCurrentTimestamp will also require an exception. Pre-emptively
        // protect the whole Uuids.timeBased method.
        "com.datastax.oss.driver.api.core.uuid.Uuids", "timeBased");

    // continuous paging

    builder.allowBlockingCallsInside(
        "com.datastax.dse.driver.internal.core.cql.continuous.ContinuousRequestHandlerBase$NodeResponseCallback",
        "cancel");
    builder.allowBlockingCallsInside(
        "com.datastax.dse.driver.internal.core.cql.continuous.ContinuousRequestHandlerBase$NodeResponseCallback",
        "dequeueOrCreatePending");
    builder.allowBlockingCallsInside(
        "com.datastax.dse.driver.internal.core.cql.continuous.ContinuousRequestHandlerBase$NodeResponseCallback",
        "isLastResponse");
    builder.allowBlockingCallsInside(
        "com.datastax.dse.driver.internal.core.cql.continuous.ContinuousRequestHandlerBase$NodeResponseCallback",
        "onFailure");
    builder.allowBlockingCallsInside(
        "com.datastax.dse.driver.internal.core.cql.continuous.ContinuousRequestHandlerBase$NodeResponseCallback",
        "onPageTimeout");
    builder.allowBlockingCallsInside(
        "com.datastax.dse.driver.internal.core.cql.continuous.ContinuousRequestHandlerBase$NodeResponseCallback",
        "onResponse");
    builder.allowBlockingCallsInside(
        "com.datastax.dse.driver.internal.core.cql.continuous.ContinuousRequestHandlerBase$NodeResponseCallback",
        "onStreamIdAssigned");
    builder.allowBlockingCallsInside(
        "com.datastax.dse.driver.internal.core.cql.continuous.ContinuousRequestHandlerBase$NodeResponseCallback",
        "operationComplete");

    // Netty extra exceptions

    // see https://github.com/netty/netty/pull/10810
    builder.allowBlockingCallsInside("io.netty.util.HashedWheelTimer", "start");
    builder.allowBlockingCallsInside("io.netty.util.HashedWheelTimer", "stop");

    // see https://github.com/netty/netty/pull/10811
    builder.allowBlockingCallsInside("io.netty.util.concurrent.GlobalEventExecutor", "addTask");
    builder.allowBlockingCallsInside(
        "io.netty.util.concurrent.SingleThreadEventExecutor", "addTask");
  }
}
