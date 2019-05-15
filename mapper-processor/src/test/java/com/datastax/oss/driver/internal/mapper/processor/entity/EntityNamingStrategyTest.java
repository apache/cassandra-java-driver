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
package com.datastax.oss.driver.internal.mapper.processor.entity;

import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.NamingStrategy;
import com.datastax.oss.driver.api.mapper.entity.naming.NameConverter;
import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import com.datastax.oss.driver.internal.mapper.processor.MapperProcessorTest;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import javax.lang.model.element.Modifier;
import org.junit.Test;

public class EntityNamingStrategyTest extends MapperProcessorTest {

  private static final ClassName CUSTOM_CONVERTER_CLASS_NAME =
      ClassName.get("test", "CustomConverter");

  private static final TypeSpec CUSTOM_CONVERTER_CLASS =
      TypeSpec.classBuilder(CUSTOM_CONVERTER_CLASS_NAME)
          .addSuperinterface(NameConverter.class)
          .addMethod(
              MethodSpec.methodBuilder("toCassandraName")
                  .addModifiers(Modifier.PUBLIC)
                  .returns(String.class)
                  .addParameter(String.class, "javaName")
                  .addStatement("return null;") // doesn't matter, converter won't be invoked
                  .build())
          .build();

  // Common code for the entity class, to avoid repeating it in every test
  private TypeSpec.Builder entityTemplate() {
    return TypeSpec.classBuilder("TestEntity")
        .addModifiers(Modifier.PUBLIC)
        .addAnnotation(Entity.class)
        .addField(TypeName.INT, "i", Modifier.PRIVATE)
        .addMethod(
            MethodSpec.methodBuilder("setI")
                .addParameter(TypeName.INT, "i")
                .addModifiers(Modifier.PUBLIC)
                .build())
        .addMethod(
            MethodSpec.methodBuilder("getI")
                .returns(TypeName.INT)
                .addModifiers(Modifier.PUBLIC)
                .addStatement("return 0")
                .build());
  }

  @Test
  public void should_fail_if_both_convention_and_converter_specified() {
    should_fail_with_expected_error(
        "Invalid annotation configuration: "
            + "NamingStrategy must have either a 'convention' or 'customConverterClass' argument, "
            + "but not both",
        "test",
        CUSTOM_CONVERTER_CLASS,
        entityTemplate()
            .addAnnotation(
                AnnotationSpec.builder(NamingStrategy.class)
                    .addMember("convention", "$T.CASE_INSENSITIVE", NamingConvention.class)
                    .addMember("customConverterClass", "$T.class", CUSTOM_CONVERTER_CLASS_NAME)
                    .build())
            .build());
  }

  @Test
  public void should_fail_if_neither_convention_nor_converter_specified() {
    should_fail_with_expected_error(
        "Invalid annotation configuration: "
            + "NamingStrategy must have either a 'convention' or 'customConverterClass' argument",
        "test",
        CUSTOM_CONVERTER_CLASS,
        entityTemplate().addAnnotation(NamingStrategy.class).build());
  }

  @Test
  public void should_warn_if_multiple_conventions_specified() {
    should_succeed_with_expected_warning(
        "Too many naming conventions: "
            + "NamingStrategy must have at most one 'convention' argument "
            + "(will use the first one: CASE_INSENSITIVE)",
        "test",
        CUSTOM_CONVERTER_CLASS,
        entityTemplate()
            .addAnnotation(
                AnnotationSpec.builder(NamingStrategy.class)
                    .addMember(
                        "convention",
                        "{ $1T.CASE_INSENSITIVE, $1T.EXACT_CASE }",
                        NamingConvention.class)
                    .build())
            .build());
  }

  @Test
  public void should_warn_if_multiple_converters_specified() {
    should_succeed_with_expected_warning(
        "Too many custom converters: "
            + "NamingStrategy must have at most one 'customConverterClass' argument "
            + "(will use the first one: test.CustomConverter)",
        "test",
        CUSTOM_CONVERTER_CLASS,
        entityTemplate()
            .addAnnotation(
                AnnotationSpec.builder(NamingStrategy.class)
                    .addMember(
                        "customConverterClass",
                        "{ $1T.class, $1T.class }",
                        CUSTOM_CONVERTER_CLASS_NAME)
                    .build())
            .build());
  }
}
