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
import com.datastax.oss.driver.internal.mapper.MapperContext;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import java.util.ArrayList;
import java.util.List;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;

/** Generates the implementation of a {@link Mapper}-annotated interface. */
public class MapperImplementationGenerator extends FileGenerator {

  private TypeElement interfaceElement;
  private final ClassName builderName;
  private final ClassName className;

  public MapperImplementationGenerator(
      TypeElement interfaceElement, ClassName builderName, GenerationContext context) {
    super(context);
    this.interfaceElement = interfaceElement;
    className = GeneratedNames.mapperImplementation(interfaceElement);
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
    List<PartialClassGenerator> daoFactoryMethods = new ArrayList<>();
    for (Element child : interfaceElement.getEnclosedElements()) {
      if (child.getKind() == ElementKind.METHOD) {
        ExecutableElement methodElement = (ExecutableElement) child;
        try {
          DaoFactoryMethodGenerator generator =
              DaoFactoryMethodGenerator.newInstance(methodElement, context);
          if (generator != null) {
            daoFactoryMethods.add(generator);
          } else {
            context
                .getMessager()
                .error(methodElement, "Don't know what to generate for this signature");
          }
        } catch (SkipGenerationException ignored) {
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
                FieldSpec.builder(MapperContext.class, "context", Modifier.PRIVATE, Modifier.FINAL)
                    .build());

    MethodSpec.Builder constructorContents =
        MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .addParameter(MapperContext.class, "context")
            .addStatement("this.context = context");

    for (PartialClassGenerator daoFactoryMethod : daoFactoryMethods) {
      daoFactoryMethod.addConstructorInstructions(constructorContents);
      daoFactoryMethod.addMembers(classContents);
    }

    classContents.addMethod(constructorContents.build());

    return JavaFile.builder(className.packageName(), classContents.build());
  }
}
