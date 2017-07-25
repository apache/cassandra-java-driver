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
package com.datastax.oss.driver.api.core.cql;

import com.datastax.oss.driver.internal.core.cql.DefaultBatchStatement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.Arrays;
import java.util.Collections;

public class BatchStatementBuilder extends StatementBuilder<BatchStatementBuilder, BatchStatement> {

  private BatchType batchType;
  private ImmutableList.Builder<BatchableStatement<?>> statementsBuilder;
  private int statementsCount;

  public BatchStatementBuilder(BatchType batchType) {
    this.batchType = batchType;
    this.statementsBuilder = ImmutableList.builder();
  }

  public BatchStatementBuilder(BatchStatement template) {
    super(template);
    this.batchType = template.getBatchType();
    this.statementsBuilder = ImmutableList.<BatchableStatement<?>>builder().addAll(template);
    this.statementsCount = template.size();
  }

  /** @see BatchStatement#add(BatchableStatement) */
  public BatchStatementBuilder addStatement(BatchableStatement<?> statement) {
    if (statementsCount >= 0xFFFF) {
      throw new IllegalStateException(
          "Batch statement cannot contain more than " + 0xFFFF + " statements.");
    }
    statementsCount += 1;
    statementsBuilder.add(statement);
    return this;
  }

  /** @see BatchStatement#addAll(Iterable) */
  public BatchStatementBuilder addStatements(Iterable<BatchableStatement<?>> statements) {
    int delta = Iterables.size(statements);
    if (statementsCount + delta > 0xFFFF) {
      throw new IllegalStateException(
          "Batch statement cannot contain more than " + 0xFFFF + " statements.");
    }
    statementsCount += delta;
    statementsBuilder.addAll(statements);
    return this;
  }

  /** @see BatchStatement#addAll(BatchableStatement[]) */
  public BatchStatementBuilder addStatements(BatchableStatement<?>... statements) {
    return addStatements(Arrays.asList(statements));
  }

  public BatchStatementBuilder clearStatements() {
    statementsBuilder = ImmutableList.builder();
    statementsCount = 0;
    return this;
  }

  @Override
  public BatchStatement build() {
    return new DefaultBatchStatement(
        batchType,
        statementsBuilder.build(),
        configProfileName,
        configProfile,
        null,
        (customPayloadBuilder == null) ? Collections.emptyMap() : customPayloadBuilder.build(),
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }
}
