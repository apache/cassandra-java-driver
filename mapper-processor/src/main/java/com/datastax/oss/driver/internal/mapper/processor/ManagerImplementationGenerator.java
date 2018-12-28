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

import com.datastax.oss.driver.api.mapper.annotations.MappingManager;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import javax.lang.model.element.Modifier;

/** Generates the implementation of a {@link MappingManager}-annotated interface. */
public class ManagerImplementationGenerator extends ClassGenerator {

  private final ClassName superInterfaceName;
  private final ClassName builderName;
  private final ClassName className;

  public ManagerImplementationGenerator(ClassName managerInterfaceName, ClassName builderName) {
    superInterfaceName = managerInterfaceName;
    className =
        ClassName.get(
            managerInterfaceName.packageName(), managerInterfaceName.simpleName() + "_Impl");
    this.builderName = builderName;
  }

  @Override
  public ClassName getClassName() {
    return className;
  }

  @Override
  protected JavaFile.Builder getContents() {
    TypeSpec.Builder contents =
        TypeSpec.classBuilder(className)
            .addJavadoc(
                "Do not instantiate this class directly, use {@link $T} instead.", builderName)
            .addJavadoc(JAVADOC_PARAGRAPH_SEPARATOR)
            .addJavadoc(JAVADOC_GENERATED_WARNING)
            .addModifiers(Modifier.PUBLIC)
            .addSuperinterface(superInterfaceName)
            .addField(
                FieldSpec.builder(SESSION_TYPE, "session", Modifier.PRIVATE, Modifier.FINAL)
                    .build())
            .addMethod(
                MethodSpec.constructorBuilder()
                    .addModifiers(Modifier.PUBLIC)
                    .addParameter(SESSION_TYPE, "session")
                    .addStatement("this.session = session")
                    .build());
    return JavaFile.builder(className.packageName(), contents.build());
  }
}
