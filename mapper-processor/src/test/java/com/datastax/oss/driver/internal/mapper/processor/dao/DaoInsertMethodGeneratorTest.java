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

import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.squareup.javapoet.AnnotationSpec;
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
public class DaoInsertMethodGeneratorTest extends DaoMethodGeneratorTest {

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
        "Insert methods must take the entity to insert as the first parameter",
        MethodSpec.methodBuilder("insert")
            .addAnnotation(Insert.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .build(),
      },
      {
        "Insert methods must take the entity to insert as the first parameter",
        MethodSpec.methodBuilder("insert")
            .addAnnotation(Insert.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ParameterSpec.builder(String.class, "a").build())
            .build(),
      },
      {
        "Insert methods must return one of [VOID, FUTURE_OF_VOID, ENTITY, FUTURE_OF_ENTITY, "
            + "OPTIONAL_ENTITY, FUTURE_OF_OPTIONAL_ENTITY, BOOLEAN, FUTURE_OF_BOOLEAN, RESULT_SET, "
            + "BOUND_STATEMENT, FUTURE_OF_ASYNC_RESULT_SET, REACTIVE_RESULT_SET]",
        MethodSpec.methodBuilder("insert")
            .addAnnotation(Insert.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ParameterSpec.builder(ENTITY_CLASS_NAME, "entity").build())
            .returns(TypeName.INT)
            .build(),
      },
    };
  }

  @Test
  @Override
  @UseDataProvider("warningSignatures")
  public void should_succeed_with_expected_warning(String expectedWarning, MethodSpec method) {
    super.should_succeed_with_expected_warning(expectedWarning, method);
  }

  @DataProvider
  public static Object[][] warningSignatures() {
    return new Object[][] {
      {
        "Invalid ttl value: "
            + "':foo bar' is not a valid placeholder, the generated query will probably fail",
        MethodSpec.methodBuilder("insert")
            .addAnnotation(
                AnnotationSpec.builder(Insert.class).addMember("ttl", "$S", ":foo bar").build())
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ENTITY_CLASS_NAME, "entity")
            .build(),
      },
      {
        "Invalid ttl value: "
            + "'foo' is not a bind marker name and can't be parsed as a number literal either, "
            + "the generated query will probably fail",
        MethodSpec.methodBuilder("insert")
            .addAnnotation(
                AnnotationSpec.builder(Insert.class).addMember("ttl", "$S", "foo").build())
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ENTITY_CLASS_NAME, "entity")
            .build(),
      },
      {
        "Invalid timestamp value: "
            + "':foo bar' is not a valid placeholder, the generated query will probably fail",
        MethodSpec.methodBuilder("insert")
            .addAnnotation(
                AnnotationSpec.builder(Insert.class)
                    .addMember("timestamp", "$S", ":foo bar")
                    .build())
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ENTITY_CLASS_NAME, "entity")
            .build(),
      },
      {
        "Invalid timestamp value: "
            + "'foo' is not a bind marker name and can't be parsed as a number literal either, "
            + "the generated query will probably fail",
        MethodSpec.methodBuilder("insert")
            .addAnnotation(
                AnnotationSpec.builder(Insert.class).addMember("timestamp", "$S", "foo").build())
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ENTITY_CLASS_NAME, "entity")
            .build(),
      },
    };
  }
}
