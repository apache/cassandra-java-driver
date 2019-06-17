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

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.internal.mapper.processor.MapperProcessorTest;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.UUID;
import javax.lang.model.element.Modifier;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class EntityPropertyAnnotationsTest extends MapperProcessorTest {

  @Test
  @UseDataProvider("entitiesWithWarnings")
  public void should_succeed_with_expected_warning(String expectedWarning, TypeSpec entitySpec) {
    super.should_succeed_with_expected_warning(expectedWarning, "test", entitySpec);
  }

  @DataProvider
  public static Object[][] entitiesWithWarnings() {
    return new Object[][] {
      {
        "@PartitionKey should be used either on the field or the getter, but not both. "
            + "The annotation on this field will be ignored.",
        TypeSpec.classBuilder(ClassName.get("test", "Product"))
            .addAnnotation(Entity.class)
            .addField(
                FieldSpec.builder(UUID.class, "id", Modifier.PRIVATE)
                    .addAnnotation(PartitionKey.class)
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("getId")
                    .addAnnotation(PartitionKey.class)
                    .returns(UUID.class)
                    .addModifiers(Modifier.PUBLIC)
                    .addStatement("return id")
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("setId")
                    .addParameter(UUID.class, "id")
                    .addModifiers(Modifier.PUBLIC)
                    .addStatement("this.id = id")
                    .build())
            .build(),
      },
      {
        "@ClusteringColumn should be used either on the field or the getter, but not both. "
            + "The annotation on this field will be ignored.",
        TypeSpec.classBuilder(ClassName.get("test", "Product"))
            .addAnnotation(Entity.class)
            .addField(
                FieldSpec.builder(UUID.class, "id", Modifier.PRIVATE)
                    .addAnnotation(ClusteringColumn.class)
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("getId")
                    .addAnnotation(ClusteringColumn.class)
                    .returns(UUID.class)
                    .addModifiers(Modifier.PUBLIC)
                    .addStatement("return id")
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("setId")
                    .addParameter(UUID.class, "id")
                    .addModifiers(Modifier.PUBLIC)
                    .addStatement("this.id = id")
                    .build())
            .build(),
      },
    };
  }

  @Test
  @UseDataProvider("entitiesWithErrors")
  public void should_fail_with_expected_error(String expectedError, TypeSpec entitySpec) {
    super.should_fail_with_expected_error(expectedError, "test", entitySpec);
  }

  @DataProvider
  public static Object[][] entitiesWithErrors() {
    return new Object[][] {
      {
        "Properties can't be annotated with both @PartitionKey and @ClusteringColumn.",
        TypeSpec.classBuilder(ClassName.get("test", "Product"))
            .addAnnotation(Entity.class)
            .addField(
                FieldSpec.builder(UUID.class, "id", Modifier.PRIVATE)
                    .addAnnotation(PartitionKey.class)
                    .addAnnotation(ClusteringColumn.class)
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("getId")
                    .returns(UUID.class)
                    .addModifiers(Modifier.PUBLIC)
                    .addStatement("return id")
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("setId")
                    .addParameter(UUID.class, "id")
                    .addModifiers(Modifier.PUBLIC)
                    .addStatement("this.id = id")
                    .build())
            .build(),
      },
      {
        "Properties can't be annotated with both @PartitionKey and @ClusteringColumn.",
        TypeSpec.classBuilder(ClassName.get("test", "Product"))
            .addAnnotation(Entity.class)
            .addField(
                FieldSpec.builder(UUID.class, "id", Modifier.PRIVATE)
                    .addAnnotation(PartitionKey.class)
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("getId")
                    .addAnnotation(ClusteringColumn.class)
                    .returns(UUID.class)
                    .addModifiers(Modifier.PUBLIC)
                    .addStatement("return id")
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("setId")
                    .addParameter(UUID.class, "id")
                    .addModifiers(Modifier.PUBLIC)
                    .addStatement("this.id = id")
                    .build())
            .build(),
      },
      {
        "Duplicate partition key index: if multiple properties are annotated with @PartitionKey, "
            + "the annotation must be parameterized with an integer indicating the position. "
            + "Found duplicate index 0 for getId1 and getId2.",
        TypeSpec.classBuilder(ClassName.get("test", "Product"))
            .addAnnotation(Entity.class)
            .addField(
                FieldSpec.builder(UUID.class, "id1", Modifier.PRIVATE)
                    .addAnnotation(PartitionKey.class)
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("getId1")
                    .returns(UUID.class)
                    .addModifiers(Modifier.PUBLIC)
                    .addStatement("return id1")
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("setId1")
                    .addParameter(UUID.class, "id1")
                    .addModifiers(Modifier.PUBLIC)
                    .addStatement("this.id1 = id1")
                    .build())
            .addField(
                FieldSpec.builder(UUID.class, "id2", Modifier.PRIVATE)
                    .addAnnotation(PartitionKey.class)
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("getId2")
                    .returns(UUID.class)
                    .addModifiers(Modifier.PUBLIC)
                    .addStatement("return id2")
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("setId2")
                    .addParameter(UUID.class, "id2")
                    .addModifiers(Modifier.PUBLIC)
                    .addStatement("this.id2 = id2")
                    .build())
            .build(),
      },
      {
        "Duplicate clustering column index: if multiple properties are annotated with @ClusteringColumn, "
            + "the annotation must be parameterized with an integer indicating the position. "
            + "Found duplicate index 1 for getId1 and getId2.",
        TypeSpec.classBuilder(ClassName.get("test", "Product"))
            .addAnnotation(Entity.class)
            .addField(
                FieldSpec.builder(UUID.class, "id1", Modifier.PRIVATE)
                    .addAnnotation(
                        AnnotationSpec.builder(ClusteringColumn.class)
                            .addMember("value", "1")
                            .build())
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("getId1")
                    .returns(UUID.class)
                    .addModifiers(Modifier.PUBLIC)
                    .addStatement("return id1")
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("setId1")
                    .addParameter(UUID.class, "id1")
                    .addModifiers(Modifier.PUBLIC)
                    .addStatement("this.id1 = id1")
                    .build())
            .addField(
                FieldSpec.builder(UUID.class, "id2", Modifier.PRIVATE)
                    .addAnnotation(
                        AnnotationSpec.builder(ClusteringColumn.class)
                            .addMember("value", "1")
                            .build())
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("getId2")
                    .returns(UUID.class)
                    .addModifiers(Modifier.PUBLIC)
                    .addStatement("return id2")
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("setId2")
                    .addParameter(UUID.class, "id2")
                    .addModifiers(Modifier.PUBLIC)
                    .addStatement("this.id2 = id2")
                    .build())
            .build(),
      },
    };
  }
}
