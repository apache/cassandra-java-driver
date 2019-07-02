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

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.internal.mapper.processor.MapperProcessorTest;
import com.squareup.javapoet.AnnotationSpec;
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
  protected static final ClassName SALE_ENTITY_CLASS_NAME = ClassName.get("test", "ProductSale");
  protected static final TypeSpec SALE_ENTITY_SPEC =
      TypeSpec.classBuilder(SALE_ENTITY_CLASS_NAME)
          .addModifiers(Modifier.PUBLIC)
          .addAnnotation(Entity.class)
          .addField(UUID.class, "id", Modifier.PRIVATE)
          .addField(String.class, "day", Modifier.PRIVATE)
          .addField(UUID.class, "customerId", Modifier.PRIVATE)
          .addField(UUID.class, "ts", Modifier.PRIVATE)
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
          .addMethod(
              MethodSpec.methodBuilder("setDay")
                  .addParameter(String.class, "day")
                  .addModifiers(Modifier.PUBLIC)
                  .addStatement("this.day = day")
                  .build())
          .addMethod(
              MethodSpec.methodBuilder("getDay")
                  .addAnnotation(
                      AnnotationSpec.builder(PartitionKey.class)
                          .addMember("value", "$L", 1)
                          .build())
                  .returns(String.class)
                  .addModifiers(Modifier.PUBLIC)
                  .addStatement("return day")
                  .build())
          .addMethod(
              MethodSpec.methodBuilder("setCustomerId")
                  .addParameter(UUID.class, "customerId")
                  .addModifiers(Modifier.PUBLIC)
                  .addStatement("this.customerId = customerId")
                  .build())
          .addMethod(
              MethodSpec.methodBuilder("getCustomerId")
                  .addAnnotation(
                      AnnotationSpec.builder(ClusteringColumn.class)
                          .addMember("value", "$L", 0)
                          .build())
                  .returns(UUID.class)
                  .addModifiers(Modifier.PUBLIC)
                  .addStatement("return customerId")
                  .build())
          .addMethod(
              MethodSpec.methodBuilder("setTs")
                  .addParameter(UUID.class, "ts")
                  .addModifiers(Modifier.PUBLIC)
                  .addStatement("this.ts = ts")
                  .build())
          .addMethod(
              MethodSpec.methodBuilder("getTs")
                  .addAnnotation(
                      AnnotationSpec.builder(ClusteringColumn.class)
                          .addMember("value", "$L", 1)
                          .build())
                  .returns(UUID.class)
                  .addModifiers(Modifier.PUBLIC)
                  .addStatement("return ts")
                  .build())
          .build();

  protected void should_fail_with_expected_error(String expectedError, MethodSpec method) {
    should_fail_with_expected_error(expectedError, method, ENTITY_SPEC);
  }

  protected void should_fail_with_expected_error(
      String expectedError, MethodSpec method, TypeSpec entitySpec) {
    should_fail_with_expected_error(
        expectedError,
        "test",
        entitySpec,
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
