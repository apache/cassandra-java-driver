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

import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.MethodSpec;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.UUID;
import javax.lang.model.element.Modifier;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DaoQueryMethodGeneratorTest extends DaoMethodGeneratorTest {

  @Test
  @Override
  @UseDataProvider("invalidSignatures")
  public void should_fail_with_expected_error(String expectedError, MethodSpec method) {
    super.should_fail_with_expected_error(expectedError, method);
  }

  @DataProvider
  public static Object[][] invalidSignatures() {
    // Not many error cases to cover, the return type/parameters are pretty open
    return new Object[][] {
      {
        "Invalid return type: Query methods must return one of [VOID, BOOLEAN, LONG, ROW, "
            + "ENTITY, OPTIONAL_ENTITY, RESULT_SET, BOUND_STATEMENT, PAGING_ITERABLE, FUTURE_OF_VOID, "
            + "FUTURE_OF_BOOLEAN, FUTURE_OF_LONG, FUTURE_OF_ROW, "
            + "FUTURE_OF_ENTITY, FUTURE_OF_OPTIONAL_ENTITY, "
            + "FUTURE_OF_ASYNC_RESULT_SET, FUTURE_OF_ASYNC_PAGING_ITERABLE, "
            + "REACTIVE_RESULT_SET, MAPPED_REACTIVE_RESULT_SET, "
            + "STREAM, FUTURE_OF_STREAM]",
        MethodSpec.methodBuilder("select")
            .addAnnotation(
                AnnotationSpec.builder(Query.class)
                    .addMember("value", "$S", "SELECT * FROM whatever")
                    .build())
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(UUID.class)
            .build(),
      },
    };
  }
}
