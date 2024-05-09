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
package com.datastax.oss.driver.example.guava.internal;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.core.session.RequestProcessorIT;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.cql.CqlRequestAsyncProcessor;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.RequestProcessor;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;

/**
 * A request processor that takes a given {@link KeyRequest#getKey} and generates a query, delegates
 * it to {@link CqlRequestAsyncProcessor} to get the integer value of a row and return it as a
 * result.
 */
public class KeyRequestProcessor implements RequestProcessor<KeyRequest, Integer> {

  public static final GenericType<Integer> INT_TYPE = GenericType.of(Integer.class);

  private final CqlRequestAsyncProcessor subProcessor;

  KeyRequestProcessor(CqlRequestAsyncProcessor subProcessor) {
    this.subProcessor = subProcessor;
  }

  @Override
  public boolean canProcess(Request request, GenericType<?> resultType) {
    return request instanceof KeyRequest && resultType.equals(INT_TYPE);
  }

  @Override
  public Integer process(
      KeyRequest request,
      DefaultSession session,
      InternalDriverContext context,
      String sessionLogPrefix) {

    // Create statement from key and delegate it to CqlRequestSyncProcessor
    SimpleStatement statement =
        SimpleStatement.newInstance(
            "select v1 from test where k = ? and v0 = ?", RequestProcessorIT.KEY, request.getKey());
    AsyncResultSet result =
        CompletableFutures.getUninterruptibly(
            subProcessor.process(statement, session, context, sessionLogPrefix));
    // If not exactly 1 rows were found, return Integer.MIN_VALUE, otherwise return the value.
    if (result.remaining() != 1) {
      return Integer.MIN_VALUE;
    } else {
      return result.currentPage().iterator().next().getInt("v1");
    }
  }

  @Override
  public Integer newFailure(RuntimeException error) {
    throw error;
  }
}
