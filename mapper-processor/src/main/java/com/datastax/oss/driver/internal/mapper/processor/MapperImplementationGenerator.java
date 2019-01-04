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

import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import java.util.ArrayList;
import java.util.List;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;

/** Generates the implementation of a {@link Mapper}-annotated interface. */
public class MapperImplementationGenerator extends FileGenerator {

  private TypeElement interfaceElement;
  private final ClassName builderName;
  private final ClassName className;

  public MapperImplementationGenerator(
      TypeElement interfaceElement, ClassName builderName, GenerationContext context) {
    super(context);
    this.interfaceElement = interfaceElement;
    className =
        ClassName.get(interfaceElement).peerClass(interfaceElement.getSimpleName() + "_Impl");
    this.builderName = builderName;
  }

  @Override
  protected String getFileName() {
    return className.packageName() + "." + className.simpleName();
  }

  public ClassName getGeneratedClassName() {
    return className;
  }

  @Override
  protected JavaFile.Builder getContents() {
    // Find all interface methods that return mappers
    List<MapperMethod> mapperMethods = new ArrayList<>();
    for (Element child : interfaceElement.getEnclosedElements()) {
      if (child.getKind() == ElementKind.METHOD) {
        ExecutableElement methodElement = (ExecutableElement) child;
        TypeMirror returnType = methodElement.getReturnType();
        ClassName mapperImplementationName = context.getGeneratedDaos().get(returnType);
        if (mapperImplementationName != null) {
          MapperMethod mapperMethod =
              new MapperMethod(methodElement, returnType, mapperImplementationName);
          context.getMessager().warn("add %s", mapperMethod);
          mapperMethods.add(mapperMethod);
        }
      }
    }

    TypeSpec.Builder classContents =
        TypeSpec.classBuilder(className)
            .addJavadoc(
                "Do not instantiate this class directly, use {@link $T} instead.", builderName)
            .addJavadoc(JAVADOC_PARAGRAPH_SEPARATOR)
            .addJavadoc(JAVADOC_GENERATED_WARNING)
            .addModifiers(Modifier.PUBLIC)
            .addSuperinterface(ClassName.get(interfaceElement))
            .addField(
                FieldSpec.builder(Session.class, "session", Modifier.PRIVATE, Modifier.FINAL)
                    .build());

    MethodSpec.Builder constructorContents =
        MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .addParameter(Session.class, "session")
            .addStatement("this.session = session");

    for (MapperMethod method : mapperMethods) {
      String methodName = method.overriddenMethodElement.getSimpleName().toString();
      String fieldName = methodName + "Instance";
      TypeName returnTypeName = ClassName.get(method.returnType);
      classContents
          .addField(
              FieldSpec.builder(returnTypeName, fieldName, Modifier.PRIVATE, Modifier.FINAL)
                  .build())
          .addMethod(
              MethodSpec.methodBuilder(methodName)
                  .addAnnotation(Override.class)
                  .addModifiers(Modifier.PUBLIC)
                  .returns(returnTypeName)
                  .addStatement("return $L", fieldName)
                  .build());
      constructorContents.addStatement(
          "this.$L = new $T(session)", fieldName, method.implementationName);
    }

    classContents.addMethod(constructorContents.build());

    return JavaFile.builder(className.packageName(), classContents.build());
  }

  static class MapperMethod {

    final ExecutableElement overriddenMethodElement;
    final TypeMirror returnType;
    final TypeName implementationName;

    MapperMethod(
        ExecutableElement overriddenMethodElement,
        TypeMirror returnType,
        TypeName implementationName) {
      this.overriddenMethodElement = overriddenMethodElement;
      this.returnType = returnType;
      this.implementationName = implementationName;
    }
  }
}
