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
package com.datastax.oss.driver.internal.mapper.processor.dao;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.lang.model.element.Modifier;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DaoSelectMethodGeneratorTest extends DaoMethodGeneratorTest {

  @Test
  @Override
  @UseDataProvider("invalidSignatures")
  public void should_fail_with_expected_error(String expectedError, MethodSpec method) {
    super.should_fail_with_expected_error(expectedError, method);
  }

  @DataProvider
  public static Object[][] invalidSignatures() {
    return new Object[][] {
      {
        "Invalid return type: Select methods must return an Entity-annotated class, or a "
            + "CompletionStage, CompletableFuture, PagingIterable or "
            + "CompletionStage/CompletableFuture<MappedAsyncPagingIterable> thereof",
        MethodSpec.methodBuilder("select")
            .addAnnotation(Select.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(Integer.class)
            .build(),
      },
      {
        "Invalid return type: Select methods must return an Entity-annotated class, or a "
            + "CompletionStage, CompletableFuture, PagingIterable or "
            + "CompletionStage/CompletableFuture<MappedAsyncPagingIterable> thereof",
        MethodSpec.methodBuilder("select")
            .addAnnotation(Select.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(ParameterizedTypeName.get(CompletionStage.class, Integer.class))
            .build(),
      },
      {
        "Select methods that don't use a custom clause must take the partition key components "
            + "in the exact order (expected PK of Product: [java.util.UUID])",
        MethodSpec.methodBuilder("select")
            .addAnnotation(Select.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(ENTITY_CLASS_NAME)
            .build(),
      },
      {
        "Select methods that don't use a custom clause must take the partition key components "
            + "in the exact order (expected PK of Product: [java.util.UUID])",
        MethodSpec.methodBuilder("select")
            .addAnnotation(Select.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(String.class, "id")
            .returns(ENTITY_CLASS_NAME)
            .build(),
      },
      {
        "Select methods that don't use a custom clause must take the partition key components "
            + "in the exact order (expected PK of Product: [java.util.UUID])",
        MethodSpec.methodBuilder("select")
            .addAnnotation(Select.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(UUID.class, "id")
            .addParameter(String.class, "extra")
            .returns(ENTITY_CLASS_NAME)
            .build(),
      },
    };
  }

  @Test
  @Override
  @UseDataProvider("validSignatures")
  public void should_succeed_without_warnings(MethodSpec method) {
    super.should_succeed_without_warnings(method);
  }

  @DataProvider
  public static Object[][] validSignatures() {
    return new Object[][] {
      {
        MethodSpec.methodBuilder("select")
            .addAnnotation(Select.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(UUID.class, "id")
            .returns(ENTITY_CLASS_NAME)
            .build()
      },
      {
        MethodSpec.methodBuilder("select")
            .addAnnotation(Select.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(UUID.class, "id")
            .returns(
                ParameterizedTypeName.get(ClassName.get(CompletionStage.class), ENTITY_CLASS_NAME))
            .build()
      },
      {
        MethodSpec.methodBuilder("select")
            .addAnnotation(Select.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(UUID.class, "id")
            .returns(
                ParameterizedTypeName.get(
                    ClassName.get(CompletableFuture.class), ENTITY_CLASS_NAME))
            .build()
      },
      {
        MethodSpec.methodBuilder("select")
            .addAnnotation(Select.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(UUID.class, "id")
            .returns(
                ParameterizedTypeName.get(ClassName.get(PagingIterable.class), ENTITY_CLASS_NAME))
            .build()
      },
      {
        MethodSpec.methodBuilder("select")
            .addAnnotation(Select.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(UUID.class, "id")
            .returns(
                ParameterizedTypeName.get(
                    ClassName.get(CompletionStage.class),
                    ParameterizedTypeName.get(
                        ClassName.get(MappedAsyncPagingIterable.class), ENTITY_CLASS_NAME)))
            .build()
      },
      {
        MethodSpec.methodBuilder("select")
            .addAnnotation(Select.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(UUID.class, "id")
            .returns(
                ParameterizedTypeName.get(
                    ClassName.get(CompletableFuture.class),
                    ParameterizedTypeName.get(
                        ClassName.get(MappedAsyncPagingIterable.class), ENTITY_CLASS_NAME)))
            .build()
      },
      {
        MethodSpec.methodBuilder("select")
            .addAnnotation(
                AnnotationSpec.builder(Select.class)
                    .addMember("customWhereClause", "\"WHERE a = :a AND b = :b\"")
                    .build())
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            // Note that we don't validate the count, names or types of the parameters, they must
            // match but that's the user's responsibility
            .addParameter(String.class, "c")
            .addParameter(Integer.class, "d")
            .addParameter(Long.class, "e")
            .returns(
                ParameterizedTypeName.get(
                    ClassName.get(CompletableFuture.class),
                    ParameterizedTypeName.get(
                        ClassName.get(MappedAsyncPagingIterable.class), ENTITY_CLASS_NAME)))
            .build()
      },
    };
  }
}
