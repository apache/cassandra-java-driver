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
package com.datastax.oss.driver.internal.mapper.processor;

import static com.google.testing.compile.CompilationSubject.assertThat;

import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.google.testing.compile.Compilation;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeSpec;
import javax.lang.model.element.Modifier;
import javax.tools.StandardLocation;
import org.junit.Test;

/** Checks that the mapper correctly processes nested annotated types. */
public class MapperAnnotationTest extends MapperProcessorTest {

  @Test
  public void should_work_on_nested_interface() {
    Compilation compilation =
        compileWithMapperProcessor(
            "test",
            TypeSpec.classBuilder(ClassName.get("test", "Foo"))
                .addModifiers(Modifier.PUBLIC)
                .addType(
                    TypeSpec.interfaceBuilder("Bar")
                        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                        .addAnnotation(Mapper.class)
                        .build())
                .build());

    assertThat(compilation).succeededWithoutWarnings();
    assertThat(compilation)
        .generatedFile(StandardLocation.SOURCE_OUTPUT, "test", "Foo_BarImpl__MapperGenerated.java")
        .contentsAsUtf8String()
        .contains("class Foo_BarImpl__MapperGenerated implements Foo.Bar");
    assertThat(compilation)
        .generatedFile(StandardLocation.SOURCE_OUTPUT, "test", "Foo_BarBuilder.java");
  }

  @Test
  public void should_fail_on_class() {
    Compilation compilation =
        compileWithMapperProcessor(
            "test",
            TypeSpec.classBuilder(ClassName.get("test", "Foo"))
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(Mapper.class)
                .build());

    assertThat(compilation)
        .hadErrorContaining("Only INTERFACE elements can be annotated with Mapper");
  }
}
