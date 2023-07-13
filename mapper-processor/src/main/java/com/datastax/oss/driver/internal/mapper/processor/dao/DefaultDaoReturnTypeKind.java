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
package com.datastax.oss.driver.internal.mapper.processor.dao;

import com.datastax.dse.driver.internal.core.cql.reactive.FailedReactiveResultSet;
import com.datastax.dse.driver.internal.mapper.reactive.FailedMappedReactiveResultSet;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.squareup.javapoet.CodeBlock;

public enum DefaultDaoReturnTypeKind implements DaoReturnTypeKind {
  VOID {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      // Note that the execute* methods in the generated code are defined in DaoBase
      methodBuilder.addStatement("execute(boundStatement)");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(CodeBlock innerBlock) {
      return innerBlock;
    }
  },
  BOOLEAN {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement("return executeAndMapWasAppliedToBoolean(boundStatement)");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(CodeBlock innerBlock) {
      return innerBlock;
    }
  },
  LONG {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement("return executeAndMapFirstColumnToLong(boundStatement)");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(CodeBlock innerBlock) {
      return innerBlock;
    }
  },
  ROW {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement("return executeAndExtractFirstRow(boundStatement)");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(CodeBlock innerBlock) {
      return innerBlock;
    }
  },
  ENTITY {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement(
          "return executeAndMapToSingleEntity(boundStatement, $L)", helperFieldName);
    }

    @Override
    public CodeBlock wrapWithErrorHandling(CodeBlock innerBlock) {
      return innerBlock;
    }
  },
  OPTIONAL_ENTITY {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement(
          "return executeAndMapToOptionalEntity(boundStatement, $L)", helperFieldName);
    }

    @Override
    public CodeBlock wrapWithErrorHandling(CodeBlock innerBlock) {
      return innerBlock;
    }
  },
  RESULT_SET {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement("return execute(boundStatement)");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(CodeBlock innerBlock) {
      return innerBlock;
    }
  },
  BOUND_STATEMENT {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement("return boundStatement");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(CodeBlock innerBlock) {
      return innerBlock;
    }
  },
  PAGING_ITERABLE {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement(
          "return executeAndMapToEntityIterable(boundStatement, $L)", helperFieldName);
    }

    @Override
    public CodeBlock wrapWithErrorHandling(CodeBlock innerBlock) {
      return innerBlock;
    }
  },

  FUTURE_OF_VOID {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement("return executeAsyncAndMapToVoid(boundStatement)");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(CodeBlock innerBlock) {
      return wrapWithErrorHandling(innerBlock, FAILED_FUTURE);
    }
  },
  FUTURE_OF_BOOLEAN {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement("return executeAsyncAndMapWasAppliedToBoolean(boundStatement)");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(CodeBlock innerBlock) {
      return wrapWithErrorHandling(innerBlock, FAILED_FUTURE);
    }
  },
  FUTURE_OF_LONG {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement("return executeAsyncAndMapFirstColumnToLong(boundStatement)");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(CodeBlock innerBlock) {
      return wrapWithErrorHandling(innerBlock, FAILED_FUTURE);
    }
  },
  FUTURE_OF_ROW {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement("return executeAsyncAndExtractFirstRow(boundStatement)");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(CodeBlock innerBlock) {
      return wrapWithErrorHandling(innerBlock, FAILED_FUTURE);
    }
  },
  FUTURE_OF_ENTITY {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement(
          "return executeAsyncAndMapToSingleEntity(boundStatement, $L)", helperFieldName);
    }

    @Override
    public CodeBlock wrapWithErrorHandling(CodeBlock innerBlock) {
      return wrapWithErrorHandling(innerBlock, FAILED_FUTURE);
    }
  },
  FUTURE_OF_OPTIONAL_ENTITY {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement(
          "return executeAsyncAndMapToOptionalEntity(boundStatement, $L)", helperFieldName);
    }

    @Override
    public CodeBlock wrapWithErrorHandling(CodeBlock innerBlock) {
      return wrapWithErrorHandling(innerBlock, FAILED_FUTURE);
    }
  },
  FUTURE_OF_ASYNC_RESULT_SET {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement("return executeAsync(boundStatement)");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(CodeBlock innerBlock) {
      return wrapWithErrorHandling(innerBlock, FAILED_FUTURE);
    }
  },
  FUTURE_OF_ASYNC_PAGING_ITERABLE {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement(
          "return executeAsyncAndMapToEntityIterable(boundStatement, $L)", helperFieldName);
    }

    @Override
    public CodeBlock wrapWithErrorHandling(CodeBlock innerBlock) {
      return wrapWithErrorHandling(innerBlock, FAILED_FUTURE);
    }
  },
  REACTIVE_RESULT_SET {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement("return executeReactive(boundStatement)");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(CodeBlock innerBlock) {
      return wrapWithErrorHandling(innerBlock, FAILED_REACTIVE_RESULT_SET);
    }
  },
  MAPPED_REACTIVE_RESULT_SET {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement(
          "return executeReactiveAndMap(boundStatement, $L)", helperFieldName);
    }

    @Override
    public CodeBlock wrapWithErrorHandling(CodeBlock innerBlock) {
      return wrapWithErrorHandling(innerBlock, FAILED_MAPPED_REACTIVE_RESULT_SET);
    }
  },

  UNSUPPORTED() {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      throw new AssertionError("Should never get here");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(CodeBlock innerBlock) {
      throw new AssertionError("Should never get here");
    }
  },
  ;

  @Override
  public String getDescription() {
    return name();
  }

  static CodeBlock wrapWithErrorHandling(CodeBlock innerBlock, CodeBlock catchBlock) {
    return CodeBlock.builder()
        .beginControlFlow("try")
        .add(innerBlock)
        .nextControlFlow("catch ($T t)", Throwable.class)
        .addStatement(catchBlock)
        .endControlFlow()
        .build();
  }

  private static final CodeBlock FAILED_FUTURE =
      CodeBlock.of("return $T.failedFuture(t)", CompletableFutures.class);
  private static final CodeBlock FAILED_REACTIVE_RESULT_SET =
      CodeBlock.of("return new $T(t)", FailedReactiveResultSet.class);
  private static final CodeBlock FAILED_MAPPED_REACTIVE_RESULT_SET =
      CodeBlock.of("return new $T(t)", FailedMappedReactiveResultSet.class);
}
