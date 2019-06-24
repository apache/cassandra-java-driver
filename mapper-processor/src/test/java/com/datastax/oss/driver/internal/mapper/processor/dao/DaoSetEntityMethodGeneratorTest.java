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

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.mapper.annotations.SetEntity;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import javax.lang.model.element.Modifier;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DaoSetEntityMethodGeneratorTest extends DaoMethodGeneratorTest {

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
        "Wrong number of parameters: SetEntity methods must have two",
        MethodSpec.methodBuilder("set")
            .addAnnotation(SetEntity.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ParameterSpec.builder(String.class, "a").build())
            .build(),
      },
      {
        "Wrong number of parameters: SetEntity methods must have two",
        MethodSpec.methodBuilder("set")
            .addAnnotation(SetEntity.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ParameterSpec.builder(String.class, "a").build())
            .addParameter(ParameterSpec.builder(String.class, "b").build())
            .addParameter(ParameterSpec.builder(String.class, "c").build())
            .build(),
      },
      {
        "Wrong parameter types: SetEntity methods must take a SettableByName and an annotated entity (in any order)",
        MethodSpec.methodBuilder("set")
            .addAnnotation(SetEntity.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ParameterSpec.builder(ENTITY_CLASS_NAME, "entity").build())
            .addParameter(ParameterSpec.builder(Integer.class, "target").build())
            .build(),
      },
      {
        "Wrong parameter types: SetEntity methods must take a SettableByName and an annotated entity (in any order)",
        MethodSpec.methodBuilder("set")
            .addAnnotation(SetEntity.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ParameterSpec.builder(String.class, "entity").build())
            .addParameter(ParameterSpec.builder(BoundStatement.class, "target").build())
            .build(),
      },
      {
        "Invalid return type: SetEntity methods must either be void, or return the same type as their settable parameter "
            + "(in this case, com.datastax.oss.driver.api.core.cql.BoundStatement to match 'target')",
        MethodSpec.methodBuilder("set")
            .addAnnotation(SetEntity.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ParameterSpec.builder(ENTITY_CLASS_NAME, "entity").build())
            .addParameter(ParameterSpec.builder(BoundStatement.class, "target").build())
            .returns(Integer.class)
            .build(),
      },
      // Return type is a SettableByName, but not the same subtype as the parameter:
      {
        "Invalid return type: SetEntity methods must either be void, or return the same type as their settable parameter "
            + "(in this case, com.datastax.oss.driver.api.core.cql.BoundStatement to match 'target')",
        MethodSpec.methodBuilder("set")
            .addAnnotation(SetEntity.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ParameterSpec.builder(ENTITY_CLASS_NAME, "entity").build())
            .addParameter(ParameterSpec.builder(BoundStatement.class, "target").build())
            .returns(UdtValue.class)
            .build(),
      },
    };
  }

  @Test
  public void should_warn_when_void_and_target_is_bound_statement() {
    super.should_succeed_with_expected_warning(
        "BoundStatement is immutable, "
            + "this method will not modify 'target' in place. "
            + "It should probably return BoundStatement rather than void",
        MethodSpec.methodBuilder("set")
            .addAnnotation(SetEntity.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(ParameterSpec.builder(ENTITY_CLASS_NAME, "entity").build())
            .addParameter(ParameterSpec.builder(BoundStatement.class, "target").build())
            .build());
  }
}
