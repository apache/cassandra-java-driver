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
import com.datastax.oss.driver.internal.mapper.processor.util.generation.GeneratedCodePatterns;
import com.datastax.oss.driver.shaded.guava.common.base.Throwables;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.TypeName;
import java.util.Map;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;

public enum DefaultDaoReturnTypeKind implements DaoReturnTypeKind {
  VOID {
    @Override
    public void addExecuteStatement(
        CodeBlock.Builder methodBuilder,
        String helperFieldName,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      // Note that the execute* methods in the generated code are defined in DaoBase
      methodBuilder.addStatement("execute(boundStatement)");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(
        CodeBlock innerBlock,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      return innerBlock;
    }
  },
  BOOLEAN {
    @Override
    public void addExecuteStatement(
        CodeBlock.Builder methodBuilder,
        String helperFieldName,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      methodBuilder.addStatement("return executeAndMapWasAppliedToBoolean(boundStatement)");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(
        CodeBlock innerBlock,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      return innerBlock;
    }
  },
  LONG {
    @Override
    public void addExecuteStatement(
        CodeBlock.Builder methodBuilder,
        String helperFieldName,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      methodBuilder.addStatement("return executeAndMapFirstColumnToLong(boundStatement)");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(
        CodeBlock innerBlock,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      return innerBlock;
    }
  },
  ROW {
    @Override
    public void addExecuteStatement(
        CodeBlock.Builder methodBuilder,
        String helperFieldName,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      methodBuilder.addStatement("return executeAndExtractFirstRow(boundStatement)");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(
        CodeBlock innerBlock,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      return innerBlock;
    }
  },
  ENTITY {
    @Override
    public void addExecuteStatement(
        CodeBlock.Builder methodBuilder,
        String helperFieldName,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      methodBuilder.addStatement(
          "return executeAndMapToSingleEntity(boundStatement, $L)", helperFieldName);
    }

    @Override
    public CodeBlock wrapWithErrorHandling(
        CodeBlock innerBlock,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      return innerBlock;
    }
  },
  OPTIONAL_ENTITY {
    @Override
    public void addExecuteStatement(
        CodeBlock.Builder methodBuilder,
        String helperFieldName,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      methodBuilder.addStatement(
          "return executeAndMapToOptionalEntity(boundStatement, $L)", helperFieldName);
    }

    @Override
    public CodeBlock wrapWithErrorHandling(
        CodeBlock innerBlock,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      return innerBlock;
    }
  },
  RESULT_SET {
    @Override
    public void addExecuteStatement(
        CodeBlock.Builder methodBuilder,
        String helperFieldName,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      methodBuilder.addStatement("return execute(boundStatement)");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(
        CodeBlock innerBlock,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      return innerBlock;
    }
  },
  BOUND_STATEMENT {
    @Override
    public void addExecuteStatement(
        CodeBlock.Builder methodBuilder,
        String helperFieldName,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      methodBuilder.addStatement("return boundStatement");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(
        CodeBlock innerBlock,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      return innerBlock;
    }
  },
  PAGING_ITERABLE {
    @Override
    public void addExecuteStatement(
        CodeBlock.Builder methodBuilder,
        String helperFieldName,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      methodBuilder.addStatement(
          "return executeAndMapToEntityIterable(boundStatement, $L)", helperFieldName);
    }

    @Override
    public CodeBlock wrapWithErrorHandling(
        CodeBlock innerBlock,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      return innerBlock;
    }
  },

  FUTURE_OF_VOID {
    @Override
    public void addExecuteStatement(
        CodeBlock.Builder methodBuilder,
        String helperFieldName,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      methodBuilder.addStatement("return executeAsyncAndMapToVoid(boundStatement)");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(
        CodeBlock innerBlock,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      return wrapWithErrorHandling(innerBlock, FAILED_FUTURE);
    }
  },
  FUTURE_OF_BOOLEAN {
    @Override
    public void addExecuteStatement(
        CodeBlock.Builder methodBuilder,
        String helperFieldName,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      methodBuilder.addStatement("return executeAsyncAndMapWasAppliedToBoolean(boundStatement)");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(
        CodeBlock innerBlock,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      return wrapWithErrorHandling(innerBlock, FAILED_FUTURE);
    }
  },
  FUTURE_OF_LONG {
    @Override
    public void addExecuteStatement(
        CodeBlock.Builder methodBuilder,
        String helperFieldName,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      methodBuilder.addStatement("return executeAsyncAndMapFirstColumnToLong(boundStatement)");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(
        CodeBlock innerBlock,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      return wrapWithErrorHandling(innerBlock, FAILED_FUTURE);
    }
  },
  FUTURE_OF_ROW {
    @Override
    public void addExecuteStatement(
        CodeBlock.Builder methodBuilder,
        String helperFieldName,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      methodBuilder.addStatement("return executeAsyncAndExtractFirstRow(boundStatement)");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(
        CodeBlock innerBlock,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      return wrapWithErrorHandling(innerBlock, FAILED_FUTURE);
    }
  },
  FUTURE_OF_ENTITY {
    @Override
    public void addExecuteStatement(
        CodeBlock.Builder methodBuilder,
        String helperFieldName,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      methodBuilder.addStatement(
          "return executeAsyncAndMapToSingleEntity(boundStatement, $L)", helperFieldName);
    }

    @Override
    public CodeBlock wrapWithErrorHandling(
        CodeBlock innerBlock,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      return wrapWithErrorHandling(innerBlock, FAILED_FUTURE);
    }
  },
  FUTURE_OF_OPTIONAL_ENTITY {
    @Override
    public void addExecuteStatement(
        CodeBlock.Builder methodBuilder,
        String helperFieldName,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      methodBuilder.addStatement(
          "return executeAsyncAndMapToOptionalEntity(boundStatement, $L)", helperFieldName);
    }

    @Override
    public CodeBlock wrapWithErrorHandling(
        CodeBlock innerBlock,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      return wrapWithErrorHandling(innerBlock, FAILED_FUTURE);
    }
  },
  FUTURE_OF_ASYNC_RESULT_SET {
    @Override
    public void addExecuteStatement(
        CodeBlock.Builder methodBuilder,
        String helperFieldName,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      methodBuilder.addStatement("return executeAsync(boundStatement)");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(
        CodeBlock innerBlock,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      return wrapWithErrorHandling(innerBlock, FAILED_FUTURE);
    }
  },
  FUTURE_OF_ASYNC_PAGING_ITERABLE {
    @Override
    public void addExecuteStatement(
        CodeBlock.Builder methodBuilder,
        String helperFieldName,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      methodBuilder.addStatement(
          "return executeAsyncAndMapToEntityIterable(boundStatement, $L)", helperFieldName);
    }

    @Override
    public CodeBlock wrapWithErrorHandling(
        CodeBlock innerBlock,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      return wrapWithErrorHandling(innerBlock, FAILED_FUTURE);
    }
  },
  REACTIVE_RESULT_SET {
    @Override
    public void addExecuteStatement(
        CodeBlock.Builder methodBuilder,
        String helperFieldName,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      methodBuilder.addStatement("return executeReactive(boundStatement)");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(
        CodeBlock innerBlock,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      return wrapWithErrorHandling(innerBlock, FAILED_REACTIVE_RESULT_SET);
    }
  },
  MAPPED_REACTIVE_RESULT_SET {
    @Override
    public void addExecuteStatement(
        CodeBlock.Builder methodBuilder,
        String helperFieldName,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      methodBuilder.addStatement(
          "return executeReactiveAndMap(boundStatement, $L)", helperFieldName);
    }

    @Override
    public CodeBlock wrapWithErrorHandling(
        CodeBlock innerBlock,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      return wrapWithErrorHandling(innerBlock, FAILED_MAPPED_REACTIVE_RESULT_SET);
    }
  },

  CUSTOM {
    @Override
    public void addExecuteStatement(
        CodeBlock.Builder methodBuilder,
        String helperFieldName,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      TypeName returnTypeName =
          GeneratedCodePatterns.getTypeName(methodElement.getReturnType(), typeParameters);
      methodBuilder
          .addStatement(
              "@$1T(\"unchecked\") $2T result =\n($2T) producer.execute(boundStatement, context, $3L)",
              SuppressWarnings.class,
              returnTypeName,
              helperFieldName)
          .addStatement("return result");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(
        CodeBlock innerBlock,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {

      TypeName returnTypeName =
          GeneratedCodePatterns.getTypeName(methodElement.getReturnType(), typeParameters);

      // We're wrapping the whole DAO method with a catch block that calls producer.wrapError.
      // wrapError can itself throw, so it's wrapped in a nested try-catch:
      CodeBlock.Builder callWrapError =
          CodeBlock.builder()
              .beginControlFlow("try")
              .addStatement(
                  "@$1T(\"unchecked\") $2T result =\n($2T) producer.wrapError(e)",
                  SuppressWarnings.class,
                  returnTypeName)
              .addStatement("return result");

      // Any exception that is explicitly declared by the DAO method can be rethrown directly.
      // (note: manually a multi-catch would be cleaner, but from here it's simpler to generate
      // separate clauses)
      for (TypeMirror thrownType : methodElement.getThrownTypes()) {
        callWrapError.nextControlFlow("catch ($T e2)", thrownType).addStatement("throw e2");
      }

      // Otherwise, rethrow unchecked exceptions and wrap checked ones.
      callWrapError
          .nextControlFlow("catch ($T e2)", Exception.class)
          .addStatement("$T.throwIfUnchecked(e2)", Throwables.class)
          .addStatement("throw new $T(e2)", RuntimeException.class)
          .endControlFlow();

      return wrapWithErrorHandling(innerBlock, callWrapError.build());
    }
  },

  UNSUPPORTED() {
    @Override
    public void addExecuteStatement(
        CodeBlock.Builder methodBuilder,
        String helperFieldName,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      throw new AssertionError("Should never get here");
    }

    @Override
    public CodeBlock wrapWithErrorHandling(
        CodeBlock innerBlock,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      throw new AssertionError("Should never get here");
    }
  },

  STREAM {
    @Override
    public void addExecuteStatement(
        CodeBlock.Builder methodBuilder,
        String helperFieldName,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      methodBuilder.addStatement(
          "return executeAndMapToEntityStream(boundStatement, $L)", helperFieldName);
    }

    @Override
    public CodeBlock wrapWithErrorHandling(
        CodeBlock innerBlock,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      return innerBlock;
    }
  },

  FUTURE_OF_STREAM {
    @Override
    public void addExecuteStatement(
        CodeBlock.Builder methodBuilder,
        String helperFieldName,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      methodBuilder.addStatement(
          "return executeAsyncAndMapToEntityStream(boundStatement, $L)", helperFieldName);
    }

    @Override
    public CodeBlock wrapWithErrorHandling(
        CodeBlock innerBlock,
        ExecutableElement methodElement,
        Map<Name, TypeElement> typeParameters) {
      return wrapWithErrorHandling(innerBlock, FAILED_FUTURE);
    }
  },
  ;

  @Override
  public String getDescription() {
    return name();
  }

  @Override
  public boolean requiresReactive() {
    return this == REACTIVE_RESULT_SET || this == MAPPED_REACTIVE_RESULT_SET;
  }

  static CodeBlock wrapWithErrorHandling(CodeBlock innerBlock, CodeBlock catchBlock) {
    return CodeBlock.builder()
        .beginControlFlow("try")
        .add(innerBlock)
        .nextControlFlow("catch ($T e)", Exception.class)
        .add(catchBlock)
        .endControlFlow()
        .build();
  }

  private static final CodeBlock FAILED_FUTURE =
      CodeBlock.builder()
          .addStatement("return $T.failedFuture(e)", CompletableFutures.class)
          .build();
  private static final CodeBlock FAILED_REACTIVE_RESULT_SET =
      CodeBlock.builder().addStatement("return new $T(e)", FailedReactiveResultSet.class).build();
  private static final CodeBlock FAILED_MAPPED_REACTIVE_RESULT_SET =
      CodeBlock.builder()
          .addStatement("return new $T(e)", FailedMappedReactiveResultSet.class)
          .build();
}
