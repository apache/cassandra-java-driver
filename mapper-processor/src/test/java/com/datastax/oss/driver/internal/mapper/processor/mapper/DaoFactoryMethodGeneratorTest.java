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

import static com.google.testing.compile.CompilationSubject.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DaoTable;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.internal.mapper.processor.MapperProcessorTest;
import com.google.testing.compile.Compilation;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.lang.model.element.Modifier;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DaoFactoryMethodGeneratorTest extends MapperProcessorTest {

  // Dummy DAO interface that is reused across tests
  private static final ClassName DAO_CLASS_NAME = ClassName.get("test", "ProductDao");
  private static final TypeSpec DAO_SPEC =
      TypeSpec.interfaceBuilder(DAO_CLASS_NAME)
          .addModifiers(Modifier.PUBLIC)
          .addAnnotation(Dao.class)
          .build();

  @Test
  @UseDataProvider("invalidSignatures")
  public void should_fail_with_expected_error(MethodSpec method, String expectedError) {
    TypeSpec mapperSpec =
        TypeSpec.interfaceBuilder(ClassName.get("test", "InventoryMapper"))
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Mapper.class)
            .addMethod(method)
            .build();

    Compilation compilation = compileWithMapperProcessor("test", DAO_SPEC, mapperSpec);

    assertThat(compilation).hadErrorContaining(expectedError);
  }

  @DataProvider
  public static Object[][] invalidSignatures() {
    return new Object[][] {
      {
        MethodSpec.methodBuilder("productDao")
            .addAnnotation(DaoFactory.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(TypeName.INT)
            .build(),
        "Invalid return type, expecting a Dao-annotated interface, or future thereof"
      },
      {
        MethodSpec.methodBuilder("productDao")
            .addAnnotation(DaoFactory.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(DAO_CLASS_NAME)
            .addParameter(String.class, "table")
            .build(),
        "Only parameters annotated with @DaoKeyspace or @DaoTable are allowed"
      },
      {
        MethodSpec.methodBuilder("productDao")
            .addAnnotation(DaoFactory.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(DAO_CLASS_NAME)
            .addParameter(
                ParameterSpec.builder(String.class, "table1").addAnnotation(DaoTable.class).build())
            .addParameter(
                ParameterSpec.builder(String.class, "table2").addAnnotation(DaoTable.class).build())
            .build(),
        "Only one parameter can be annotated with @DaoTable"
      },
      {
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
        "Only one parameter can be annotated with @DaoKeyspace"
      },
      {
        MethodSpec.methodBuilder("productDao")
            .addAnnotation(DaoFactory.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(DAO_CLASS_NAME)
            .addParameter(
                ParameterSpec.builder(Integer.class, "table").addAnnotation(DaoTable.class).build())
            .build(),
        "@DaoTable-annotated parameter must be of type String or CqlIdentifier"
      },
      {
        MethodSpec.methodBuilder("productDao")
            .addAnnotation(DaoFactory.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(DAO_CLASS_NAME)
            .addParameter(
                ParameterSpec.builder(Integer.class, "keyspace")
                    .addAnnotation(DaoKeyspace.class)
                    .build())
            .build(),
        "@DaoKeyspace-annotated parameter must be of type String or CqlIdentifier"
      },
    };
  }

  @Test
  @UseDataProvider("validSignatures")
  public void should_succeed_without_warnings(MethodSpec method) {
    TypeSpec mapperSpec =
        TypeSpec.interfaceBuilder(ClassName.get("test", "InventoryMapper"))
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Mapper.class)
            .addMethod(method)
            .build();

    Compilation compilation = compileWithMapperProcessor("test", DAO_SPEC, mapperSpec);

    assertThat(compilation).succeededWithoutWarnings();
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
