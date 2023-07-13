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

import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.squareup.javapoet.CodeBlock;

public enum DefaultDaoReturnTypeKind implements DaoReturnTypeKind {
  VOID(false) {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      // Note that the execute* methods in the generated code are defined in DaoBase
      methodBuilder.addStatement("execute(boundStatement)");
    }
  },
  BOOLEAN(false) {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement("return executeAndMapWasAppliedToBoolean(boundStatement)");
    }
  },
  LONG(false) {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement("return executeAndMapFirstColumnToLong(boundStatement)");
    }
  },
  ROW(false) {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement("return executeAndExtractFirstRow(boundStatement)");
    }
  },
  ENTITY(false) {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement(
          "return executeAndMapToSingleEntity(boundStatement, $L)", helperFieldName);
    }
  },
  OPTIONAL_ENTITY(false) {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement(
          "return executeAndMapToOptionalEntity(boundStatement, $L)", helperFieldName);
    }
  },
  RESULT_SET(false) {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement("return execute(boundStatement)");
    }
  },
  PAGING_ITERABLE(false) {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement(
          "return executeAndMapToEntityIterable(boundStatement, $L)", helperFieldName);
    }
  },

  FUTURE_OF_VOID(true) {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement("return executeAsyncAndMapToVoid(boundStatement)");
    }
  },
  FUTURE_OF_BOOLEAN(true) {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement("return executeAsyncAndMapWasAppliedToBoolean(boundStatement)");
    }
  },
  FUTURE_OF_LONG(true) {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement("return executeAsyncAndMapFirstColumnToLong(boundStatement)");
    }
  },
  FUTURE_OF_ROW(true) {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement("return executeAsyncAndExtractFirstRow(boundStatement)");
    }
  },
  FUTURE_OF_ENTITY(true) {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement(
          "return executeAsyncAndMapToSingleEntity(boundStatement, $L)", helperFieldName);
    }
  },
  FUTURE_OF_OPTIONAL_ENTITY(true) {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement(
          "return executeAsyncAndMapToOptionalEntity(boundStatement, $L)", helperFieldName);
    }
  },
  FUTURE_OF_ASYNC_RESULT_SET(true) {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement("return executeAsync(boundStatement)");
    }
  },
  FUTURE_OF_ASYNC_PAGING_ITERABLE(true) {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      methodBuilder.addStatement(
          "return executeAsyncAndMapToEntityIterable(boundStatement, $L)", helperFieldName);
    }
  },

  UNSUPPORTED(false) {
    @Override
    public void addExecuteStatement(CodeBlock.Builder methodBuilder, String helperFieldName) {
      throw new AssertionError("Should never get here");
    }
  },
  ;

  private final boolean isAsync;

  DefaultDaoReturnTypeKind(boolean isAsync) {
    this.isAsync = isAsync;
  }

  @Override
  public String getDescription() {
    return name();
  }

  @Override
  public CodeBlock wrapWithErrorHandling(CodeBlock innerBlock) {
    if (isAsync) {
      return CodeBlock.builder()
          .beginControlFlow("try")
          .add(innerBlock)
          .nextControlFlow("catch ($T t)", Throwable.class)
          .addStatement("return $T.failedFuture(t)", CompletableFutures.class)
          .endControlFlow()
          .build();
    } else {
      return innerBlock;
    }
  }
}
