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

import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
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
            + "FUTURE_OF_ENTITY, FUTURE_OF_OPTIONAL_ENTITY, PAGING_ITERABLE, STREAM, "
            + "FUTURE_OF_ASYNC_PAGING_ITERABLE, FUTURE_OF_STREAM, MAPPED_REACTIVE_RESULT_SET]",
        MethodSpec.methodBuilder("select")
            .addAnnotation(Select.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(Integer.class)
            .build(),
      },
      {
        "Invalid return type: Select methods must return one of [ENTITY, OPTIONAL_ENTITY, "
            + "FUTURE_OF_ENTITY, FUTURE_OF_OPTIONAL_ENTITY, PAGING_ITERABLE, STREAM, "
            + "FUTURE_OF_ASYNC_PAGING_ITERABLE, FUTURE_OF_STREAM, MAPPED_REACTIVE_RESULT_SET]",
        MethodSpec.methodBuilder("select")
            .addAnnotation(Select.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(ParameterizedTypeName.get(CompletionStage.class, Integer.class))
            .build(),
      },
      {
        "Select methods that don't use a custom clause must match the primary key components "
            + "in the exact order (expected primary key of Product: [java.util.UUID]). Mismatch "
            + "at index 0: java.lang.String should be java.util.UUID",
        MethodSpec.methodBuilder("select")
            .addAnnotation(Select.class)
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .addParameter(String.class, "id")
            .returns(ENTITY_CLASS_NAME)
            .build(),
      },
    };
  }
}
