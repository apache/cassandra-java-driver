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

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import javax.lang.model.element.Modifier;

public class ManagerBuilderGenerator extends ClassGenerator {

  private final ClassName builderName;
  private final ClassName interfaceName;
  private final ClassName implementationName;

  public ManagerBuilderGenerator(
      ClassName builderName, ClassName interfaceName, ClassName implementationName) {
    this.builderName = builderName;
    this.interfaceName = interfaceName;
    this.implementationName = implementationName;
  }

  @Override
  protected ClassName getClassName() {
    return builderName;
  }

  @Override
  protected JavaFile.Builder getContents() {
    TypeSpec.Builder contents =
        TypeSpec.classBuilder(builderName)
            .addJavadoc(
                "Builds an instance of {@link $T} wrapping a driver {@link $T}.",
                interfaceName,
                SESSION_TYPE)
            .addJavadoc(JAVADOC_PARAGRAPH_SEPARATOR)
            .addJavadoc(JAVADOC_GENERATED_WARNING)
            .addModifiers(Modifier.PUBLIC)
            .addField(
                FieldSpec.builder(SESSION_TYPE, "session", Modifier.PRIVATE, Modifier.FINAL)
                    .build())
            .addMethod(
                MethodSpec.constructorBuilder()
                    .addModifiers(Modifier.PUBLIC)
                    .addParameter(SESSION_TYPE, "session")
                    .addStatement("this.session = session")
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("build")
                    .addModifiers(Modifier.PUBLIC)
                    .returns(interfaceName)
                    .addStatement("return new $T(session)", implementationName)
                    .build());
    return JavaFile.builder(builderName.packageName(), contents.build());
  }
}
