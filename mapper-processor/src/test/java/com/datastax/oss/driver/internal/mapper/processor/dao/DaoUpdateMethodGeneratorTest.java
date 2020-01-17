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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.TypeName;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import javax.lang.model.element.Modifier;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DaoUpdateMethodGeneratorTest extends DaoMethodGeneratorTest {

  private static final AnnotationSpec UPDATE_ANNOTATION =
      AnnotationSpec.builder(Update.class).build();

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
        "Update methods must take the entity to update as the first parameter",
        MethodSpec.methodBuilder("update")
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addAnnotation(UPDATE_ANNOTATION)
            .build(),
      },
      {
        "Update methods must take the entity to update as the first parameter",
        MethodSpec.methodBuilder("update")
            .addAnnotation(UPDATE_ANNOTATION)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ParameterSpec.builder(String.class, "a").build())
            .build(),
      },
      {
        "Invalid return type: Update methods must return one of [VOID, FUTURE_OF_VOID, "
            + "RESULT_SET, BOUND_STATEMENT, FUTURE_OF_ASYNC_RESULT_SET, BOOLEAN, "
            + "FUTURE_OF_BOOLEAN, REACTIVE_RESULT_SET]",
        MethodSpec.methodBuilder("update")
            .addAnnotation(UPDATE_ANNOTATION)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ParameterSpec.builder(ENTITY_CLASS_NAME, "entity").build())
            .returns(TypeName.INT)
            .build(),
      },
      {
        "Invalid annotation parameters: Update cannot have both ifExists and customIfClause",
        MethodSpec.methodBuilder("update")
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addAnnotation(
                AnnotationSpec.builder(Update.class)
                    .addMember("ifExists", "true")
                    .addMember("customIfClause", "$S", "1 = 1")
                    .build())
            .addParameter(ParameterSpec.builder(ENTITY_CLASS_NAME, "entity").build())
            .returns(TypeName.VOID)
            .build(),
      },
    };
  }

  @Test
  public void should_warn_when_non_bind_marker_has_cql_name() {
    should_succeed_with_expected_warning(
        "Parameter entity does not refer "
            + "to a bind marker, @CqlName annotation will be ignored",
        MethodSpec.methodBuilder("update")
            .addAnnotation(
                AnnotationSpec.builder(Update.class)
                    .addMember("customIfClause", "$S", "description LIKE :searchString")
                    .build())
            .addParameter(
                ParameterSpec.builder(ENTITY_CLASS_NAME, "entity")
                    .addAnnotation(
                        AnnotationSpec.builder(CqlName.class)
                            .addMember("value", "$S", "irrelevant")
                            .build())
                    .build())
            .addParameter(
                ParameterSpec.builder(String.class, "searchString")
                    .addAnnotation(
                        AnnotationSpec.builder(CqlName.class)
                            .addMember("value", "$S", "irrelevant")
                            .build())
                    .build())
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .build());
  }

  @Test
  @UseDataProvider("usingTimestampProvider")
  public void should_process_timestamp(String timestamp, CodeBlock expected) {
    // given
    ProcessorContext processorContext = mock(ProcessorContext.class);
    DaoUpdateMethodGenerator daoUpdateMethodGenerator =
        new DaoUpdateMethodGenerator(null, null, null, null, processorContext);
    MethodSpec.Builder builder = MethodSpec.constructorBuilder();

    // when
    daoUpdateMethodGenerator.maybeAddTimestamp(timestamp, builder);

    // then
    assertThat(builder.build().code).isEqualTo(expected);
  }

  @Test
  @UseDataProvider("usingTtlProvider")
  public void should_process_ttl(String ttl, CodeBlock expected) {
    // given
    ProcessorContext processorContext = mock(ProcessorContext.class);
    DaoUpdateMethodGenerator daoUpdateMethodGenerator =
        new DaoUpdateMethodGenerator(null, null, null, null, processorContext);
    MethodSpec.Builder builder = MethodSpec.constructorBuilder();

    // when
    daoUpdateMethodGenerator.maybeAddTtl(ttl, builder);

    // then
    assertThat(builder.build().code).isEqualTo(expected);
  }

  @DataProvider
  public static Object[][] usingTimestampProvider() {
    return new Object[][] {
      {"1", CodeBlock.of(".usingTimestamp(1)")},
      {
        ":ts", CodeBlock.of(".usingTimestamp($T.bindMarker($S))", QueryBuilder.class, "ts"),
      },
      {"1", CodeBlock.of(".usingTimestamp(1)")},
      {
        ":TS", CodeBlock.of(".usingTimestamp($T.bindMarker($S))", QueryBuilder.class, "TS"),
      },
    };
  }

  @DataProvider
  public static Object[][] usingTtlProvider() {
    return new Object[][] {
      {"1", CodeBlock.of(".usingTtl(1)")},
      {
        ":ttl", CodeBlock.of(".usingTtl($T.bindMarker($S))", QueryBuilder.class, "ttl"),
      },
      {"1", CodeBlock.of(".usingTtl(1)")},
      {
        ":TTL", CodeBlock.of(".usingTtl($T.bindMarker($S))", QueryBuilder.class, "TTL"),
      },
    };
  }
}
