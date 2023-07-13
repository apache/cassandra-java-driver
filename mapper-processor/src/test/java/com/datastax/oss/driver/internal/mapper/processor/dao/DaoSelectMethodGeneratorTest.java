/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.mapper.processor.dao;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.UUID;
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
        "Invalid return type: Select methods must return one of [ENTITY, OPTIONAL_ENTITY, "
            + "FUTURE_OF_ENTITY, FUTURE_OF_OPTIONAL_ENTITY, PAGING_ITERABLE, "
            + "FUTURE_OF_ASYNC_PAGING_ITERABLE]",
        MethodSpec.methodBuilder("select")
            .addAnnotation(Select.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(Integer.class)
            .build(),
      },
      {
        "Invalid return type: Select methods must return one of [ENTITY, OPTIONAL_ENTITY, "
            + "FUTURE_OF_ENTITY, FUTURE_OF_OPTIONAL_ENTITY, PAGING_ITERABLE, "
            + "FUTURE_OF_ASYNC_PAGING_ITERABLE]",
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
  public void should_warn_when_non_bind_marker_has_cql_name() {
    should_succeed_with_expected_warning(
        "Method select(java.util.UUID): parameter id does not refer "
            + "to a bind marker, @CqlName annotation will be ignored",
        MethodSpec.methodBuilder("select")
            .addAnnotation(Select.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(
                ParameterSpec.builder(UUID.class, "id")
                    .addAnnotation(
                        AnnotationSpec.builder(CqlName.class)
                            .addMember("value", "$S", "irrelevant")
                            .build())
                    .build())
            .returns(ENTITY_CLASS_NAME)
            .build());
  }
}
