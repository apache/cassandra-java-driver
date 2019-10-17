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

import static com.google.testing.compile.CompilationSubject.assertThat;

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveResultSet;
import com.datastax.dse.driver.api.mapper.reactive.MappedReactiveResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.google.testing.compile.Compilation;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Collections;
import java.util.UUID;
import javax.lang.model.element.Modifier;
import javax.tools.StandardLocation;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DaoImplementationGeneratorTest extends DaoMethodGeneratorTest {

  private static final ClassName REACTIVE_RESULT_CLASS_NAME =
      ClassName.get(ReactiveResultSet.class);

  private static final ClassName MAPPED_REACTIVE_RESULT_CLASS_NAME =
      ClassName.get(MappedReactiveResultSet.class);

  private static final ParameterizedTypeName ENTITY_MAPPED_REACTIVE_RESULT_SET =
      ParameterizedTypeName.get(MAPPED_REACTIVE_RESULT_CLASS_NAME, ENTITY_CLASS_NAME);

  @Test
  public void should_fail_if_method_is_not_annotated() {
    should_fail_with_expected_error(
        "Unrecognized method signature: no implementation will be generated",
        MethodSpec.methodBuilder("get")
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ParameterSpec.builder(Row.class, "source").build())
            .returns(ENTITY_CLASS_NAME)
            .build());
  }

  @Test
  public void should_ignore_static_methods() {
    should_succeed_without_warnings(
        MethodSpec.methodBuilder("doNothing")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .build());
  }

  @Test
  public void should_ignore_default_methods() {
    should_succeed_without_warnings(
        MethodSpec.methodBuilder("doNothing")
            .addModifiers(Modifier.PUBLIC, Modifier.DEFAULT)
            .build());
  }

  @Test
  public void should_compile_with_logging_enabled() {
    Compilation compilation =
        compileWithMapperProcessor(
            "test",
            Collections.emptyList(),
            ENTITY_SPEC,
            TypeSpec.interfaceBuilder(ClassName.get("test", "ProductDao"))
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(Dao.class)
                .addMethod(
                    MethodSpec.methodBuilder("update")
                        .addAnnotation(Update.class)
                        .addParameter(ENTITY_CLASS_NAME, "product")
                        .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                        .build())
                .build());

    assertThat(compilation).succeededWithoutWarnings();
    assertGeneratedFileContains(
        compilation,
        "private static final Logger LOG = LoggerFactory.getLogger(ProductDaoImpl__MapperGenerated.class);");
    assertGeneratedFileContains(compilation, "LOG.debug(\"[{}] Initializing new instance");
    assertGeneratedFileContains(compilation, "LOG.debug(\"[{}] Preparing query `{}` for method");
  }

  @Test
  @UseDataProvider("disabledLoggingOptions")
  public void should_compile_with_logging_disabled(Iterable<?> options) {
    Compilation compilation =
        compileWithMapperProcessor(
            "test",
            options,
            ENTITY_SPEC,
            TypeSpec.interfaceBuilder(ClassName.get("test", "ProductDao"))
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(Dao.class)
                .addMethod(
                    MethodSpec.methodBuilder("update")
                        .addAnnotation(Update.class)
                        .addParameter(ENTITY_CLASS_NAME, "product")
                        .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                        .build())
                .build());
    assertThat(compilation).succeededWithoutWarnings();
    assertGeneratedFileDoesNotContain(compilation, "LoggerFactory.getLogger");
    assertGeneratedFileDoesNotContain(compilation, "LOG.debug");
  }

  @Test
  public void should_generate_findById_method_returning_MappedReactiveResultSet() {
    Compilation compilation =
        compileWithMapperProcessor(
            "test",
            Collections.emptyList(),
            ENTITY_SPEC,
            TypeSpec.interfaceBuilder(ClassName.get("test", "ProductDao"))
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(Dao.class)
                .addMethod(
                    MethodSpec.methodBuilder("findById")
                        .addAnnotation(Select.class)
                        .addParameter(ParameterSpec.builder(UUID.class, "pk").build())
                        .returns(ENTITY_MAPPED_REACTIVE_RESULT_SET)
                        .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                        .build())
                .build());
    assertThat(compilation).succeededWithoutWarnings();
    assertGeneratedFileContains(
        compilation, "public MappedReactiveResultSet<Product> findById(UUID pk)");
    assertGeneratedFileContains(
        compilation, "return executeReactiveAndMap(boundStatement, productHelper);");
  }

  @Test
  public void should_generate_insert_method_returning_ReactiveResultSet() {
    Compilation compilation =
        compileWithMapperProcessor(
            "test",
            Collections.emptyList(),
            ENTITY_SPEC,
            TypeSpec.interfaceBuilder(ClassName.get("test", "ProductDao"))
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(Dao.class)
                .addMethod(
                    MethodSpec.methodBuilder("insertIfNotExists")
                        .addAnnotation(Insert.class)
                        .addParameter(ParameterSpec.builder(ENTITY_CLASS_NAME, "product").build())
                        .returns(REACTIVE_RESULT_CLASS_NAME)
                        .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                        .build())
                .build());
    assertThat(compilation).succeededWithoutWarnings();
    assertGeneratedFileContains(
        compilation, "public ReactiveResultSet insertIfNotExists(Product product)");
    assertGeneratedFileContains(compilation, "return executeReactive(boundStatement);");
  }

  @Test
  public void should_generate_update_method_returning_ReactiveResultSet() {
    Compilation compilation =
        compileWithMapperProcessor(
            "test",
            Collections.emptyList(),
            ENTITY_SPEC,
            TypeSpec.interfaceBuilder(ClassName.get("test", "ProductDao"))
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(Dao.class)
                .addMethod(
                    MethodSpec.methodBuilder("updateIfExists")
                        .addAnnotation(Update.class)
                        .addParameter(ParameterSpec.builder(ENTITY_CLASS_NAME, "product").build())
                        .returns(REACTIVE_RESULT_CLASS_NAME)
                        .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                        .build())
                .build());
    assertThat(compilation).succeededWithoutWarnings();
    assertGeneratedFileContains(
        compilation, "public ReactiveResultSet updateIfExists(Product product)");
    assertGeneratedFileContains(compilation, "return executeReactive(boundStatement);");
  }

  @Test
  public void should_generate_delete_method_returning_ReactiveResultSet() {
    Compilation compilation =
        compileWithMapperProcessor(
            "test",
            Collections.emptyList(),
            ENTITY_SPEC,
            TypeSpec.interfaceBuilder(ClassName.get("test", "ProductDao"))
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(Dao.class)
                .addMethod(
                    MethodSpec.methodBuilder("delete")
                        .addAnnotation(Delete.class)
                        .addParameter(ParameterSpec.builder(ENTITY_CLASS_NAME, "product").build())
                        .returns(REACTIVE_RESULT_CLASS_NAME)
                        .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                        .build())
                .build());
    assertThat(compilation).succeededWithoutWarnings();
    assertGeneratedFileContains(compilation, "public ReactiveResultSet delete(Product product)");
    assertGeneratedFileContains(compilation, "return executeReactive(boundStatement);");
  }

  @Test
  public void should_generate_query_method_returning_ReactiveResultSet() {
    Compilation compilation =
        compileWithMapperProcessor(
            "test",
            Collections.emptyList(),
            ENTITY_SPEC,
            TypeSpec.interfaceBuilder(ClassName.get("test", "ProductDao"))
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(Dao.class)
                .addMethod(
                    MethodSpec.methodBuilder("queryReactive")
                        .addAnnotation(
                            AnnotationSpec.builder(Query.class)
                                .addMember("value", "$S", "SELECT * FROM whatever")
                                .build())
                        .returns(REACTIVE_RESULT_CLASS_NAME)
                        .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                        .build())
                .build());
    assertThat(compilation).succeededWithoutWarnings();
    assertGeneratedFileContains(compilation, "public ReactiveResultSet queryReactive()");
    assertGeneratedFileContains(compilation, "return executeReactive(boundStatement);");
  }

  @Test
  public void should_generate_query_method_returning_MappedReactiveResultSet() {
    Compilation compilation =
        compileWithMapperProcessor(
            "test",
            Collections.emptyList(),
            ENTITY_SPEC,
            TypeSpec.interfaceBuilder(ClassName.get("test", "ProductDao"))
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(Dao.class)
                .addMethod(
                    MethodSpec.methodBuilder("queryReactiveMapped")
                        .addAnnotation(
                            AnnotationSpec.builder(Query.class)
                                .addMember("value", "$S", "SELECT * FROM whatever")
                                .build())
                        .returns(ENTITY_MAPPED_REACTIVE_RESULT_SET)
                        .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                        .build())
                .build());
    assertThat(compilation).succeededWithoutWarnings();
    assertGeneratedFileContains(
        compilation, "public MappedReactiveResultSet<Product> queryReactiveMapped()");
    assertGeneratedFileContains(
        compilation, "return executeReactiveAndMap(boundStatement, productHelper);");
  }

  protected void assertGeneratedFileDoesNotContain(Compilation compilation, String string) {
    assertThat(compilation)
        .generatedFile(
            StandardLocation.SOURCE_OUTPUT, "test", "ProductDaoImpl__MapperGenerated.java")
        .contentsAsUtf8String()
        .doesNotContain(string);
  }

  protected void assertGeneratedFileContains(Compilation compilation, String string) {
    assertThat(compilation)
        .generatedFile(
            StandardLocation.SOURCE_OUTPUT, "test", "ProductDaoImpl__MapperGenerated.java")
        .contentsAsUtf8String()
        .contains(string);
  }

  @DataProvider
  public static Object[][] disabledLoggingOptions() {
    return new Object[][] {
      {Collections.singletonList("-Acom.datastax.oss.driver.mapper.logs.enabled=false")},
      {Collections.singletonList("-Acom.datastax.oss.driver.mapper.logs.enabled=malformed")}
    };
  }
}
