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

import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.MappingManager;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import java.util.List;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;

/** Generates the implementation of a {@link MappingManager}-annotated interface. */
public class ManagerImplementationGenerator extends ClassGenerator {

  private final ClassName interfaceName;
  private final ClassName builderName;
  private final ClassName className;
  private final List<MapperMethod> mapperMethods;

  public ManagerImplementationGenerator(
      ClassName interfaceName,
      TypeElement interfaceElement,
      ClassName builderName,
      GenerationContext context) {
    super(context);
    this.interfaceName = interfaceName;
    className = ClassName.get(interfaceName.packageName(), interfaceName.simpleName() + "_Impl");
    this.builderName = builderName;

    // Find all interface methods that return mappers
    ImmutableList.Builder<MapperMethod> mapperMethodsBuilder = ImmutableList.builder();
    for (Element child : interfaceElement.getEnclosedElements()) {
      if (child.getKind() == ElementKind.METHOD) {
        ExecutableElement method = (ExecutableElement) child;
        TypeElement returnType = getReturnType(method, context);
        if (returnType != null && returnType.getAnnotation(Mapper.class) != null) {
          if (!method.getParameters().isEmpty()) {
            context.getMessager().error(child, "Expected no arguments");
            continue;
          }
          TypeName mapperInterfaceName = ClassName.get(returnType);
          @SuppressWarnings("SuspiciousMethodCalls") // if not a ClassName, will produce null
          ClassName mapperImplementationName =
              context.getGeneratedMappers().get(mapperInterfaceName);
          if (mapperImplementationName == null) {
            context
                .getMessager()
                .error(method, "Found no implementation class matching %s", mapperInterfaceName);
            continue;
          }
          MapperMethod mapperMethod =
              new MapperMethod(
                  method.getSimpleName().toString(), mapperInterfaceName, mapperImplementationName);
          context.getMessager().warn("add %s", mapperMethod);
          mapperMethodsBuilder.add(mapperMethod);
        }
      }
    }
    mapperMethods = mapperMethodsBuilder.build();
  }

  /*
   * TODO revisit that when we deal with generic types, e.g. CompletionStage<SomeMapper>
   *
   * A generic type invocation is not a TypeElement, the mirror will be a DeclaredType.
   */
  private TypeElement getReturnType(ExecutableElement method, GenerationContext context) {
    TypeMirror mirror = method.getReturnType();
    Element element = context.getTypeUtils().asElement(mirror);
    if (element == null) {
      context.getMessager().error(method, "Could not resolve return type %s", mirror);
      return null;
    }
    if (!(element instanceof TypeElement)) {
      context.getMessager().error(method, "Return type %s should be TypeElement", element);
      return null;
    }
    return (TypeElement) element;
  }

  @Override
  public ClassName getClassName() {
    return className;
  }

  @Override
  protected JavaFile.Builder getContents() {
    TypeSpec.Builder classContents =
        TypeSpec.classBuilder(className)
            .addJavadoc(
                "Do not instantiate this class directly, use {@link $T} instead.", builderName)
            .addJavadoc(JAVADOC_PARAGRAPH_SEPARATOR)
            .addJavadoc(JAVADOC_GENERATED_WARNING)
            .addModifiers(Modifier.PUBLIC)
            .addSuperinterface(interfaceName)
            .addField(
                FieldSpec.builder(SESSION_TYPE, "session", Modifier.PRIVATE, Modifier.FINAL)
                    .build());

    MethodSpec.Builder constructorContents =
        MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .addParameter(SESSION_TYPE, "session")
            .addStatement("this.session = session");

    for (MapperMethod method : mapperMethods) {
      String fieldName = method.methodName + "Instance";
      classContents
          .addField(
              FieldSpec.builder(method.interfaceName, fieldName, Modifier.PRIVATE, Modifier.FINAL)
                  .build())
          .addMethod(
              MethodSpec.methodBuilder(method.methodName)
                  .addAnnotation(Override.class)
                  .addModifiers(Modifier.PUBLIC)
                  .returns(method.interfaceName)
                  .addStatement("return $L", fieldName)
                  .build());
      constructorContents.addStatement(
          "this.$L = new $T(session)", fieldName, method.implementationName);
    }

    classContents.addMethod(constructorContents.build());

    return JavaFile.builder(className.packageName(), classContents.build());
  }

  static class MapperMethod {
    final String methodName;
    final TypeName interfaceName;
    final TypeName implementationName;

    public MapperMethod(String methodName, TypeName interfaceName, TypeName implementationName) {
      this.methodName = methodName;
      this.interfaceName = interfaceName;
      this.implementationName = implementationName;
    }

    @Override
    public String toString() {
      return String.format("%s %s() returns %s", interfaceName, methodName, implementationName);
    }
  }
}
