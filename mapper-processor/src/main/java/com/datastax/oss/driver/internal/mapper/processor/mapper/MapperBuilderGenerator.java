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

import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.mapper.MapperContext;
import com.datastax.oss.driver.internal.mapper.processor.GeneratedNames;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.SingleFileCodeGenerator;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;

public class MapperBuilderGenerator extends SingleFileCodeGenerator {

  private final ClassName builderName;
  private final TypeElement interfaceElement;

  public MapperBuilderGenerator(TypeElement interfaceElement, ProcessorContext context) {
    super(context);
    this.builderName = GeneratedNames.mapperBuilder(interfaceElement);
    this.interfaceElement = interfaceElement;
  }

  @Override
  protected String getFileName() {
    return builderName.packageName() + "." + builderName.simpleName();
  }

  @Override
  protected JavaFile.Builder getContents() {
    TypeSpec.Builder contents =
        TypeSpec.classBuilder(builderName)
            .addJavadoc(
                "Builds an instance of {@link $T} wrapping a driver {@link $T}.",
                interfaceElement,
                Session.class)
            .addJavadoc(JAVADOC_PARAGRAPH_SEPARATOR)
            .addJavadoc(JAVADOC_GENERATED_WARNING)
            .addModifiers(Modifier.PUBLIC)
            .addField(
                FieldSpec.builder(Session.class, "session", Modifier.PRIVATE, Modifier.FINAL)
                    .build())
            .addMethod(
                MethodSpec.constructorBuilder()
                    .addModifiers(Modifier.PUBLIC)
                    .addParameter(Session.class, "session")
                    .addStatement("this.session = session")
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("build")
                    .addModifiers(Modifier.PUBLIC)
                    .returns(ClassName.get(interfaceElement))
                    .addStatement("$1T context = new $1T(session)", MapperContext.class)
                    .addStatement(
                        "return new $T(context)",
                        GeneratedNames.mapperImplementation(interfaceElement))
                    .build());
    return JavaFile.builder(builderName.packageName(), contents.build());
  }
}
