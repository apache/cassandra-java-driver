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
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.mapper.annotations.GetEntity;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.stream.Stream;
import javax.lang.model.element.Modifier;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DaoGetEntityMethodGeneratorTest extends DaoMethodGeneratorTest {

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
        "Wrong number of parameters: GetEntity methods must have exactly one",
        MethodSpec.methodBuilder("get")
            .addAnnotation(GetEntity.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .build(),
      },
      {
        "Wrong number of parameters: GetEntity methods must have exactly one",
        MethodSpec.methodBuilder("get")
            .addAnnotation(GetEntity.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ParameterSpec.builder(String.class, "a").build())
            .addParameter(ParameterSpec.builder(String.class, "b").build())
            .build(),
      },
      {
        "Invalid parameter type: GetEntity methods must take a GettableByName, ResultSet or AsyncResultSet",
        MethodSpec.methodBuilder("get")
            .addAnnotation(GetEntity.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ParameterSpec.builder(String.class, "a").build())
            .build(),
      },
      {
        "Invalid return type: GetEntity methods must return a Entity-annotated class, or a PagingIterable, a Stream or MappedAsyncPagingIterable thereof",
        MethodSpec.methodBuilder("get")
            .addAnnotation(GetEntity.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ParameterSpec.builder(Row.class, "source").build())
            .build(),
      },
      {
        "Invalid return type: GetEntity methods must return a Entity-annotated class, or a PagingIterable, a Stream or MappedAsyncPagingIterable thereof",
        MethodSpec.methodBuilder("get")
            .addAnnotation(GetEntity.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ParameterSpec.builder(ResultSet.class, "source").build())
            .returns(ParameterizedTypeName.get(PagingIterable.class, Integer.class))
            .build(),
      },
      {
        "Invalid return type: GetEntity methods returning PagingIterable must have an argument of type ResultSet",
        MethodSpec.methodBuilder("get")
            .addAnnotation(GetEntity.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ParameterSpec.builder(Row.class, "source").build())
            .returns(
                ParameterizedTypeName.get(ClassName.get(PagingIterable.class), ENTITY_CLASS_NAME))
            .build(),
      },
      {
        "Invalid return type: GetEntity methods returning Stream must have an argument of type ResultSet",
        MethodSpec.methodBuilder("get")
            .addAnnotation(GetEntity.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ParameterSpec.builder(Row.class, "source").build())
            .returns(ParameterizedTypeName.get(ClassName.get(Stream.class), ENTITY_CLASS_NAME))
            .build(),
      },
      {
        "Invalid return type: GetEntity methods returning MappedAsyncPagingIterable must have an argument of type AsyncResultSet",
        MethodSpec.methodBuilder("get")
            .addAnnotation(GetEntity.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ParameterSpec.builder(Row.class, "source").build())
            .returns(
                ParameterizedTypeName.get(
                    ClassName.get(MappedAsyncPagingIterable.class), ENTITY_CLASS_NAME))
            .build(),
      },
    };
  }
}
