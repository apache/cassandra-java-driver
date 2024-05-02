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
package com.datastax.oss.driver.internal.mapper.processor.entity;

import static com.google.testing.compile.CompilationSubject.assertThat;

import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.internal.mapper.processor.MapperProcessorTest;
import com.google.common.truth.StringSubject;
import com.google.testing.compile.Compilation;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import javax.lang.model.element.Modifier;
import javax.tools.StandardLocation;
import org.junit.Test;

public class EntityAnnotationTest extends MapperProcessorTest {

  @Test
  public void should_work_on_nested_class() {
    Compilation compilation =
        compileWithMapperProcessor(
            "test",
            TypeSpec.classBuilder(ClassName.get("test", "Foo"))
                .addModifiers(Modifier.PUBLIC)
                .addType(
                    TypeSpec.classBuilder("Bar")
                        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                        .addAnnotation(Entity.class)
                        // Dummy getter and setter to have at least one mapped property
                        .addMethod(
                            MethodSpec.methodBuilder("setI")
                                .addParameter(TypeName.INT, "i")
                                .addModifiers(Modifier.PUBLIC)
                                .build())
                        .addMethod(
                            MethodSpec.methodBuilder("getI")
                                .returns(TypeName.INT)
                                .addModifiers(Modifier.PUBLIC)
                                .addStatement("return 0")
                                .build())
                        .build())
                .build());

    assertThat(compilation).succeededWithoutWarnings();
    assertThat(compilation)
        .generatedFile(
            StandardLocation.SOURCE_OUTPUT, "test", "Foo_BarHelper__MapperGenerated.java")
        .contentsAsUtf8String()
        .contains("class Foo_BarHelper__MapperGenerated extends EntityHelperBase<Foo.Bar>");
  }

  @Test
  public void should_detect_boolean_getter() {
    Compilation compilation =
        compileWithMapperProcessor(
            "test",
            TypeSpec.classBuilder(ClassName.get("test", "Foo"))
                .addModifiers(Modifier.PUBLIC)
                .addType(
                    TypeSpec.classBuilder("Bar")
                        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                        .addAnnotation(Entity.class)
                        // Dummy getter and setter to have at least one mapped property
                        .addMethod(
                            MethodSpec.methodBuilder("setBool")
                                .addParameter(TypeName.BOOLEAN, "bool")
                                .addModifiers(Modifier.PUBLIC)
                                .build())
                        .addMethod(
                            MethodSpec.methodBuilder("isBool")
                                .returns(TypeName.BOOLEAN)
                                .addModifiers(Modifier.PUBLIC)
                                .addStatement("return true")
                                .build())
                        .build())
                .build());
    assertThat(compilation).succeededWithoutWarnings();
    StringSubject contents =
        assertThat(compilation)
            .generatedFile(
                StandardLocation.SOURCE_OUTPUT, "test", "Foo_BarHelper__MapperGenerated.java")
            .contentsAsUtf8String();
    contents.contains("target = target.setBoolean(\"bool\", entity.isBool())");
    contents.contains("boolean propertyValue = source.getBoolean(\"bool\");");
  }

  @Test
  public void should_fail_on_interface() {
    should_fail_with_expected_error(
        "Only CLASS elements can be annotated with Entity",
        "test",
        TypeSpec.interfaceBuilder(ClassName.get("test", "Foo"))
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Entity.class)
            .build());
  }
}
