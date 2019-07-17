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

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.TypeSpec;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.UUID;
import javax.lang.model.element.Modifier;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DaoDeleteMethodGeneratorTest extends DaoMethodGeneratorTest {

  @Test
  @Override
  @UseDataProvider("invalidSignatures")
  public void should_fail_with_expected_error(
      String expectedError, MethodSpec method, TypeSpec entitySpec) {
    super.should_fail_with_expected_error(expectedError, method, entitySpec);
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
        ENTITY_SPEC
      },
      {
        "Wrong number of parameters: Delete methods with no custom clause "
            + "must take either an entity instance, or the partition key components",
        MethodSpec.methodBuilder("delete")
            .addAnnotation(Delete.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .build(),
        ENTITY_SPEC
      },
      {
        "Missing entity class: Delete methods that do not operate on an entity "
            + "instance must have an 'entityClass' argument",
        MethodSpec.methodBuilder("delete")
            .addAnnotation(Delete.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(UUID.class, "id")
            .build(),
        ENTITY_SPEC
      },
      {
        "Invalid parameter list: Delete methods that do not operate on an entity instance "
            + "and lack a custom where clause must match the primary key components in the exact "
            + "order (expected primary key of Product: [java.util.UUID]). Mismatch at index 0: java.lang"
            + ".Integer should be java.util.UUID",
        MethodSpec.methodBuilder("delete")
            .addAnnotation(
                AnnotationSpec.builder(Delete.class)
                    .addMember("entityClass", "$T.class", ENTITY_CLASS_NAME)
                    .build())
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(Integer.class, "id")
            .build(),
        ENTITY_SPEC
      },
      {
        "Invalid parameter list: Delete methods that do not operate on an entity instance "
            + "and lack a custom where clause must at least specify partition key components "
            + "(expected partition key of ProductSale: [java.util.UUID, java.lang.String])",
        MethodSpec.methodBuilder("delete")
            .addAnnotation(
                AnnotationSpec.builder(Delete.class)
                    .addMember("entityClass", "$T.class", SALE_ENTITY_CLASS_NAME)
                    .build())
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(Integer.class, "id")
            .build(),
        SALE_ENTITY_SPEC
      },
      {
        "Delete methods must return one of [VOID, FUTURE_OF_VOID, BOOLEAN, FUTURE_OF_BOOLEAN, "
            + "RESULT_SET, FUTURE_OF_ASYNC_RESULT_SET]",
        MethodSpec.methodBuilder("delete")
            .addAnnotation(Delete.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ENTITY_CLASS_NAME, "entity")
            .returns(Integer.class)
            .build(),
        ENTITY_SPEC
      },
      {
        "Wrong number of parameters: Delete methods can only have additional parameters "
            + "if they specify a custom WHERE or IF clause",
        MethodSpec.methodBuilder("delete")
            .addAnnotation(Delete.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ENTITY_CLASS_NAME, "entity")
            .addParameter(Integer.class, "extra")
            .build(),
        ENTITY_SPEC
      },
      {
        "Delete methods that have a custom where clause must not take an Entity (Product) as a "
            + "parameter",
        MethodSpec.methodBuilder("delete")
            .addAnnotation(
                AnnotationSpec.builder(Delete.class)
                    .addMember("customWhereClause", "$S", "hello")
                    .build())
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ENTITY_CLASS_NAME, "entity")
            .returns(Integer.class)
            .build(),
        ENTITY_SPEC
      },
    };
  }

  @Test
  public void should_warn_when_non_bind_marker_has_cql_name() {
    should_succeed_with_expected_warning(
        "delete(java.util.UUID,java.lang.String): parameter id does not refer "
            + "to a bind marker, @CqlName annotation will be ignored",
        MethodSpec.methodBuilder("delete")
            .addAnnotation(
                AnnotationSpec.builder(Delete.class)
                    .addMember("entityClass", ENTITY_CLASS_NAME + ".class")
                    .addMember("customIfClause", "$S", "description = :description")
                    .build())
            .addParameter(
                ParameterSpec.builder(UUID.class, "id")
                    .addAnnotation(
                        AnnotationSpec.builder(CqlName.class)
                            .addMember("value", "$S", "irrelevant")
                            .build())
                    .build())
            .addParameter(String.class, "description")
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .build());
  }
}
