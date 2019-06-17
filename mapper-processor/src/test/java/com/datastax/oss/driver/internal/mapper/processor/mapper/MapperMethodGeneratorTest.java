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
package com.datastax.oss.driver.internal.mapper.processor.mapper;

import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.internal.mapper.processor.MapperProcessorTest;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import javax.lang.model.element.Modifier;

public class MapperMethodGeneratorTest extends MapperProcessorTest {

  // Dummy DAO interface that is reused across tests
  protected static final ClassName DAO_CLASS_NAME = ClassName.get("test", "ProductDao");
  protected static final TypeSpec DAO_SPEC =
      TypeSpec.interfaceBuilder(DAO_CLASS_NAME)
          .addModifiers(Modifier.PUBLIC)
          .addAnnotation(Dao.class)
          .build();

  protected void should_fail_with_expected_error(String expectedError, MethodSpec method) {
    should_fail_with_expected_error(
        expectedError,
        "test",
        DAO_SPEC,
        TypeSpec.interfaceBuilder(ClassName.get("test", "InventoryMapper"))
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Mapper.class)
            .addMethod(method)
            .build());
  }

  protected void should_succeed_without_warnings(MethodSpec method) {
    should_succeed_without_warnings(
        "test",
        DAO_SPEC,
        TypeSpec.interfaceBuilder(ClassName.get("test", "InventoryMapper"))
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Mapper.class)
            .addMethod(method)
            .build());
  }
}
