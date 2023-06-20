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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.mapper.MapperBuilder;
import com.datastax.oss.driver.internal.mapper.DefaultMapperContext;
import com.datastax.oss.driver.internal.mapper.processor.GeneratedNames;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.SingleFileCodeGenerator;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
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
  protected ClassName getPrincipalTypeName() {
    return builderName;
  }

  protected Class<? extends CqlSession> getSessionClass() {
    return CqlSession.class;
  }

  @Override
  protected JavaFile.Builder getContents() {
    TypeSpec.Builder classContents =
        TypeSpec.classBuilder(builderName)
            .superclass(
                ParameterizedTypeName.get(
                    ClassName.get(MapperBuilder.class), ClassName.get(interfaceElement)))
            .addJavadoc(
                "Builds an instance of {@link $T} wrapping a driver {@link $T}.",
                interfaceElement,
                getSessionClass())
            .addJavadoc(JAVADOC_PARAGRAPH_SEPARATOR)
            .addJavadoc(JAVADOC_GENERATED_WARNING)
            .addAnnotation(
                AnnotationSpec.builder(SuppressWarnings.class)
                    .addMember("value", "\"all\"")
                    .build())
            .addModifiers(Modifier.PUBLIC)
            .addMethod(
                MethodSpec.constructorBuilder()
                    .addModifiers(Modifier.PUBLIC)
                    .addParameter(getSessionClass(), "session")
                    .addStatement("super(session)")
                    .build())
            .addMethod(
                MethodSpec.methodBuilder("build")
                    .addModifiers(Modifier.PUBLIC)
                    .addAnnotation(Override.class)
                    .returns(ClassName.get(interfaceElement))
                    .addStatement(
                        "$1T context = new $1T(session, defaultKeyspaceId, "
                            + "defaultExecutionProfileName, defaultExecutionProfile, customState)",
                        DefaultMapperContext.class)
                    .addStatement(
                        "return new $T(context)",
                        GeneratedNames.mapperImplementation(interfaceElement))
                    .build());
    return JavaFile.builder(builderName.packageName(), classContents.build());
  }
}
