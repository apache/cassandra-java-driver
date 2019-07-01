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

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.google.testing.compile.Compilation;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.TypeSpec;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Collections;
import javax.lang.model.element.Modifier;
import javax.tools.StandardLocation;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DaoImplementationGeneratorTest extends DaoMethodGeneratorTest {

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

  private void assertGeneratedFileDoesNotContain(Compilation compilation, String string) {
    assertThat(compilation)
        .generatedFile(
            StandardLocation.SOURCE_OUTPUT, "test", "ProductDaoImpl__MapperGenerated.java")
        .contentsAsUtf8String()
        .doesNotContain(string);
  }

  private void assertGeneratedFileContains(Compilation compilation, String string) {
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
