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

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
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
public class DaoDeleteMethodGeneratorTest extends DaoMethodGeneratorTest {

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
        "Invalid annotation parameters: Delete cannot have both ifExists and customIfClause",
        MethodSpec.methodBuilder("delete")
            .addAnnotation(
                AnnotationSpec.builder(Delete.class)
                    .addMember("ifExists", "true")
                    .addMember("customIfClause", "$S", "whatever")
                    .build())
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .build(),
      },
      {
        "Wrong number of parameters: Delete methods with no custom clause "
            + "must take either an entity instance, or the partition key components",
        MethodSpec.methodBuilder("delete")
            .addAnnotation(Delete.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .build(),
      },
      {
        "Missing entity class: Delete methods that do not operate on an entity "
            + "instance must have an 'entityClass' argument",
        MethodSpec.methodBuilder("delete")
            .addAnnotation(Delete.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(UUID.class, "id")
            .build(),
      },
      {
        "Invalid parameter list: Delete methods that do not operate on an entity instance "
            + "must take the partition key components in the exact order "
            + "(expected PK of Product: [java.util.UUID])",
        MethodSpec.methodBuilder("delete")
            .addAnnotation(
                AnnotationSpec.builder(Delete.class)
                    .addMember("entityClass", "$T.class", ENTITY_CLASS_NAME)
                    .build())
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(Integer.class, "id")
            .build(),
      },
      {
        "Invalid return type: Delete methods must return void, boolean or a result set, "
            + "or a CompletableFuture/CompletionStage of Void, Boolean or AsyncResultSet",
        MethodSpec.methodBuilder("delete")
            .addAnnotation(Delete.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ENTITY_CLASS_NAME, "entity")
            .returns(Integer.class)
            .build(),
      },
      {
        "Wrong number of parameters: Delete methods can only have additional parameters "
            + "if they specify a custom IF clause",
        MethodSpec.methodBuilder("delete")
            .addAnnotation(Delete.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ENTITY_CLASS_NAME, "entity")
            .addParameter(Integer.class, "extra")
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
        MethodSpec.methodBuilder("delete")
            .addAnnotation(Delete.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ENTITY_CLASS_NAME, "entity")
            .build()
      },
      {
        MethodSpec.methodBuilder("delete")
            .addAnnotation(Delete.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ENTITY_CLASS_NAME, "entity")
            .returns(TypeName.BOOLEAN)
            .build()
      },
      {
        MethodSpec.methodBuilder("delete")
            .addAnnotation(Delete.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ENTITY_CLASS_NAME, "entity")
            .returns(Boolean.class)
            .build()
      },
      {
        MethodSpec.methodBuilder("delete")
            .addAnnotation(Delete.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ENTITY_CLASS_NAME, "entity")
            .returns(ResultSet.class)
            .build()
      },
      {
        MethodSpec.methodBuilder("delete")
            .addAnnotation(Delete.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ENTITY_CLASS_NAME, "entity")
            .returns(ParameterizedTypeName.get(CompletableFuture.class, Void.class))
            .build()
      },
      {
        MethodSpec.methodBuilder("delete")
            .addAnnotation(Delete.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ENTITY_CLASS_NAME, "entity")
            .returns(ParameterizedTypeName.get(CompletionStage.class, Boolean.class))
            .build()
      },
      {
        MethodSpec.methodBuilder("delete")
            .addAnnotation(Delete.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ENTITY_CLASS_NAME, "entity")
            .returns(ParameterizedTypeName.get(CompletableFuture.class, AsyncResultSet.class))
            .build()
      },
      {
        MethodSpec.methodBuilder("delete")
            .addAnnotation(
                AnnotationSpec.builder(Delete.class)
                    .addMember("customIfClause", "$S", "IF foo = :foo")
                    .build())
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ENTITY_CLASS_NAME, "entity")
            .addParameter(Integer.class, "foo")
            .build()
      },
    };
  }
}
