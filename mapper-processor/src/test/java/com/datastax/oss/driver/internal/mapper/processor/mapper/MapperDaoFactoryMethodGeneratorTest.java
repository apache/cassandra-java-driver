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
package com.datastax.oss.driver.internal.mapper.processor.mapper;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DaoTable;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.lang.model.element.Modifier;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class MapperDaoFactoryMethodGeneratorTest extends MapperMethodGeneratorTest {

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
        "Invalid return type: DaoFactory methods must return a Dao-annotated interface, or future thereof",
        MethodSpec.methodBuilder("productDao")
            .addAnnotation(DaoFactory.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(TypeName.INT)
            .build(),
      },
      {
        "Invalid parameter annotations: DaoFactory method parameters must be annotated with @DaoKeyspace, @DaoTable or @DaoProfile",
        MethodSpec.methodBuilder("productDao")
            .addAnnotation(DaoFactory.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(DAO_CLASS_NAME)
            .addParameter(String.class, "table")
            .build(),
      },
      {
        "Invalid parameter annotations: only one DaoFactory method parameter can be annotated with @DaoTable",
        MethodSpec.methodBuilder("productDao")
            .addAnnotation(DaoFactory.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(DAO_CLASS_NAME)
            .addParameter(
                ParameterSpec.builder(String.class, "table1").addAnnotation(DaoTable.class).build())
            .addParameter(
                ParameterSpec.builder(String.class, "table2").addAnnotation(DaoTable.class).build())
            .build(),
      },
      {
        "Invalid parameter annotations: only one DaoFactory method parameter can be annotated with @DaoKeyspace",
        MethodSpec.methodBuilder("productDao")
            .addAnnotation(DaoFactory.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(DAO_CLASS_NAME)
            .addParameter(
                ParameterSpec.builder(String.class, "keyspace1")
                    .addAnnotation(DaoKeyspace.class)
                    .build())
            .addParameter(
                ParameterSpec.builder(String.class, "table").addAnnotation(DaoTable.class).build())
            .addParameter(
                ParameterSpec.builder(String.class, "keyspace2")
                    .addAnnotation(DaoKeyspace.class)
                    .build())
            .build(),
      },
      {
        "Invalid parameter type: @DaoTable-annotated parameter of DaoFactory methods must be of type String or CqlIdentifier",
        MethodSpec.methodBuilder("productDao")
            .addAnnotation(DaoFactory.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(DAO_CLASS_NAME)
            .addParameter(
                ParameterSpec.builder(Integer.class, "table").addAnnotation(DaoTable.class).build())
            .build(),
      },
      {
        "Invalid parameter type: @DaoKeyspace-annotated parameter of DaoFactory methods must be of type String or CqlIdentifier",
        MethodSpec.methodBuilder("productDao")
            .addAnnotation(DaoFactory.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(DAO_CLASS_NAME)
            .addParameter(
                ParameterSpec.builder(Integer.class, "keyspace")
                    .addAnnotation(DaoKeyspace.class)
                    .build())
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
        MethodSpec.methodBuilder("productDao")
            .addAnnotation(DaoFactory.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(DAO_CLASS_NAME)
            .build()
      },
      {
        MethodSpec.methodBuilder("productDao")
            .addAnnotation(DaoFactory.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(
                ParameterizedTypeName.get(ClassName.get(CompletionStage.class), DAO_CLASS_NAME))
            .build()
      },
      {
        MethodSpec.methodBuilder("productDao")
            .addAnnotation(DaoFactory.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(
                ParameterizedTypeName.get(ClassName.get(CompletableFuture.class), DAO_CLASS_NAME))
            .build()
      },
      {
        MethodSpec.methodBuilder("productDao")
            .addAnnotation(DaoFactory.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(DAO_CLASS_NAME)
            .addParameter(
                ParameterSpec.builder(String.class, "keyspace")
                    .addAnnotation(DaoKeyspace.class)
                    .build())
            .addParameter(
                ParameterSpec.builder(String.class, "table").addAnnotation(DaoTable.class).build())
            .build()
      },
      {
        MethodSpec.methodBuilder("productDao")
            .addAnnotation(DaoFactory.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(DAO_CLASS_NAME)
            .addParameter(
                ParameterSpec.builder(CqlIdentifier.class, "keyspace")
                    .addAnnotation(DaoKeyspace.class)
                    .build())
            .addParameter(
                ParameterSpec.builder(CqlIdentifier.class, "table")
                    .addAnnotation(DaoTable.class)
                    .build())
            .build()
      },
      {
        MethodSpec.methodBuilder("productDao")
            .addAnnotation(DaoFactory.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(DAO_CLASS_NAME)
            .addParameter(
                ParameterSpec.builder(String.class, "table").addAnnotation(DaoTable.class).build())
            .addParameter(
                ParameterSpec.builder(CqlIdentifier.class, "keyspace")
                    .addAnnotation(DaoKeyspace.class)
                    .build())
            .build()
      },
    };
  }
}
