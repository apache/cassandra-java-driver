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

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.google.testing.compile.Compilation;
import com.google.testing.compile.Compiler;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.TypeSpec;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.tools.JavaFileObject;

public abstract class MapperProcessorTest {

  /**
   * Launches an in-process execution of javac with {@link MapperProcessor} enabled.
   *
   * @param packageName the package of the types to process. Note that it is currently not possible
   *     to process multiple packages (and it's unlikely to be needed in unit tests).
   * @param options the compiler options (use to pass -A options to the processor).
   * @param typeSpecs the contents of the classes or interfaces to process.
   */
  protected Compilation compileWithMapperProcessor(
      String packageName, Iterable<?> options, TypeSpec... typeSpecs) {
    List<JavaFileObject> files = new ArrayList<>();
    for (TypeSpec typeSpec : typeSpecs) {
      files.add(JavaFile.builder(packageName, typeSpec).build().toJavaFileObject());
    }
    return Compiler.javac()
        .withProcessors(getMapperProcessor())
        .withOptions(options)
        .compile(files);
  }

  @NonNull
  protected MapperProcessor getMapperProcessor() {
    return new MapperProcessor();
  }

  /**
   * Launches an in-process execution of javac with {@link MapperProcessor} enabled, and <b>custom
   * result types disabled</b>.
   *
   * @param packageName the package of the types to process. Note that it is currently not possible
   *     to process multiple packages (and it's unlikely to be needed in unit tests).
   * @param typeSpecs the contents of the classes or interfaces to process.
   */
  protected Compilation compileWithMapperProcessor(String packageName, TypeSpec... typeSpecs) {
    return compileWithMapperProcessor(
        packageName,
        ImmutableList.of("-Acom.datastax.oss.driver.mapper.customResults.enabled=false"),
        typeSpecs);
  }

  protected void should_fail_with_expected_error(
      String expectedError, String packageName, TypeSpec... typeSpecs) {
    Compilation compilation = compileWithMapperProcessor(packageName, typeSpecs);
    assertThat(compilation).hadErrorContaining(expectedError);
  }

  protected void should_succeed_with_expected_warning(
      String expectedWarning, String packageName, TypeSpec... typeSpecs) {
    Compilation compilation = compileWithMapperProcessor(packageName, typeSpecs);
    assertThat(compilation).hadWarningContaining(expectedWarning);
  }

  protected void should_succeed_without_warnings(String packageName, TypeSpec... typeSpecs) {
    Compilation compilation =
        compileWithMapperProcessor(packageName, Collections.emptyList(), typeSpecs);
    assertThat(compilation).succeededWithoutWarnings();
  }
}
