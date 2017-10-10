/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.api.core.session;

import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareAsyncProcessor;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareSyncProcessor;
import com.datastax.oss.driver.internal.core.cql.CqlRequestAsyncProcessor;
import com.datastax.oss.driver.internal.core.cql.CqlRequestSyncProcessor;
import com.datastax.oss.driver.internal.core.cql.DefaultPreparedStatement;
import com.datastax.oss.driver.internal.core.session.RequestProcessorRegistry;
import com.google.common.collect.MapMaker;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * A Custom {@link DefaultDriverContext} that overrides {@link #requestProcessorRegistry()} to
 * return a {@link RequestProcessorRegistry} that includes processors for returning guava futures.
 */
public class GuavaDriverContext extends DefaultDriverContext {

  GuavaDriverContext(DriverConfigLoader configLoader, List<TypeCodec<?>> typeCodecs) {
    super(configLoader, typeCodecs);
  }

  @Override
  public RequestProcessorRegistry requestProcessorRegistry() {
    // Register the typical request processors, except instead of the normal async processors,
    // use GuavaRequestAsyncProcessor to return ListenableFutures in async methods.
    ConcurrentMap<ByteBuffer, DefaultPreparedStatement> preparedStatementsCache =
        new MapMaker().weakValues().makeMap();

    CqlRequestAsyncProcessor cqlRequestAsyncProcessor = new CqlRequestAsyncProcessor();
    CqlPrepareAsyncProcessor cqlPrepareAsyncProcessor =
        new CqlPrepareAsyncProcessor(preparedStatementsCache);
    CqlRequestSyncProcessor cqlRequestSyncProcessor = new CqlRequestSyncProcessor();

    return new RequestProcessorRegistry(
        clusterName(),
        cqlRequestSyncProcessor,
        new CqlPrepareSyncProcessor(preparedStatementsCache),
        new GuavaRequestAsyncProcessor<>(
            cqlRequestAsyncProcessor, Statement.class, GuavaSession.ASYNC),
        new GuavaRequestAsyncProcessor<>(
            cqlPrepareAsyncProcessor, PrepareRequest.class, GuavaSession.ASYNC_PREPARED),
        // Register KeyRequestProcessor for handling KeyRequest and returning Integer.
        new KeyRequestProcessor(cqlRequestSyncProcessor));
  }
}
