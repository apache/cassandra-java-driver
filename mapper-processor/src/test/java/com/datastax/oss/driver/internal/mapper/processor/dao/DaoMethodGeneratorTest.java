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

import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.internal.mapper.processor.MapperProcessorTest;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import java.util.UUID;
import javax.lang.model.element.Modifier;

public abstract class DaoMethodGeneratorTest extends MapperProcessorTest {

  // Dummy entity class that can be reused across tests
  protected static final ClassName ENTITY_CLASS_NAME = ClassName.get("test", "Product");
  protected static final TypeSpec ENTITY_SPEC =
      TypeSpec.classBuilder(ENTITY_CLASS_NAME)
          .addModifiers(Modifier.PUBLIC)
          .addAnnotation(Entity.class)
          .addField(UUID.class, "id", Modifier.PRIVATE)
          .addMethod(
              MethodSpec.methodBuilder("setId")
                  .addParameter(UUID.class, "id")
                  .addModifiers(Modifier.PUBLIC)
                  .addStatement("this.id = id")
                  .build())
          .addMethod(
              MethodSpec.methodBuilder("getId")
                  .addAnnotation(PartitionKey.class)
                  .returns(UUID.class)
                  .addModifiers(Modifier.PUBLIC)
                  .addStatement("return id")
                  .build())
          .build();

  protected void should_fail_with_expected_error(String expectedError, MethodSpec method) {
    should_fail_with_expected_error(
        expectedError,
        "test",
        ENTITY_SPEC,
        TypeSpec.interfaceBuilder(ClassName.get("test", "ProductDao"))
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Dao.class)
            .addMethod(method)
            .build());
  }

  protected void should_succeed_with_expected_warning(String expectedWarning, MethodSpec method) {
    should_succeed_with_expected_warning(
        expectedWarning,
        "test",
        ENTITY_SPEC,
        TypeSpec.interfaceBuilder(ClassName.get("test", "ProductDao"))
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Dao.class)
            .addMethod(method)
            .build());
  }

  protected void should_succeed_without_warnings(MethodSpec method) {
    should_succeed_without_warnings(
        "test",
        ENTITY_SPEC,
        TypeSpec.interfaceBuilder(ClassName.get("test", "ProductDao"))
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Dao.class)
            .addMethod(method)
            .build());
  }
}
