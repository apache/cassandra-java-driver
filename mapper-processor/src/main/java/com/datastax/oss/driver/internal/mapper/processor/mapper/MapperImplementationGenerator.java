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
package com.datastax.oss.driver.internal.mapper.processor.mapper;

import com.datastax.oss.driver.internal.core.util.concurrent.LazyReference;
import com.datastax.oss.driver.internal.mapper.DaoCacheKey;
import com.datastax.oss.driver.internal.mapper.DefaultMapperContext;
import com.datastax.oss.driver.internal.mapper.processor.GeneratedNames;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.SingleFileCodeGenerator;
import com.datastax.oss.driver.internal.mapper.processor.util.NameIndex;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.GeneratedCodePatterns;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;

public class MapperImplementationGenerator extends SingleFileCodeGenerator
    implements MapperImplementationSharedCode {

  private final TypeElement interfaceElement;
  private final ClassName className;
  private final NameIndex nameIndex = new NameIndex();
  private final List<DaoSimpleField> daoSimpleFields = new ArrayList<>();
  private final List<DaoMapField> daoMapFields = new ArrayList<>();

  public MapperImplementationGenerator(TypeElement interfaceElement, ProcessorContext context) {
    super(context);
    this.interfaceElement = interfaceElement;
    className = GeneratedNames.mapperImplementation(interfaceElement);
  }

  @Override
  public String addDaoSimpleField(
      String suggestedFieldName,
      TypeName fieldType,
      TypeName daoImplementationType,
      boolean isAsync) {
    String fieldName = nameIndex.uniqueField(suggestedFieldName);
    daoSimpleFields.add(new DaoSimpleField(fieldName, fieldType, daoImplementationType, isAsync));
    return fieldName;
  }

  @Override
  public String addDaoMapField(String suggestedFieldName, TypeName mapValueType) {
    String fieldName = nameIndex.uniqueField(suggestedFieldName);
    daoMapFields.add(new DaoMapField(fieldName, mapValueType));
    return fieldName;
  }

  @Override
  protected ClassName getPrincipalTypeName() {
    return className;
  }

  @Override
  protected JavaFile.Builder getContents() {

    TypeSpec.Builder classContents =
        TypeSpec.classBuilder(className)
            .addJavadoc(
                "Do not instantiate this class directly, use {@link $T} instead.",
                GeneratedNames.mapperBuilder(interfaceElement))
            .addJavadoc(JAVADOC_PARAGRAPH_SEPARATOR)
            .addJavadoc(JAVADOC_GENERATED_WARNING)
            .addAnnotation(
                AnnotationSpec.builder(SuppressWarnings.class)
                    .addMember("value", "\"all\"")
                    .build())
            .addModifiers(Modifier.PUBLIC)
            .addSuperinterface(ClassName.get(interfaceElement));

    for (Element child : interfaceElement.getEnclosedElements()) {
      if (child.getKind() == ElementKind.METHOD) {
        ExecutableElement methodElement = (ExecutableElement) child;
        Set<Modifier> modifiers = methodElement.getModifiers();
        if (!modifiers.contains(Modifier.STATIC) && !modifiers.contains(Modifier.DEFAULT)) {
          Optional<MethodGenerator> maybeGenerator =
              context
                  .getCodeGeneratorFactory()
                  .newMapperImplementationMethod(methodElement, interfaceElement, this);
          if (!maybeGenerator.isPresent()) {
            context
                .getMessager()
                .error(
                    methodElement,
                    "Unrecognized method signature: no implementation will be generated");
          } else {
            maybeGenerator.flatMap(MethodGenerator::generate).ifPresent(classContents::addMethod);
          }
        }
      }
    }

    MethodSpec.Builder constructorContents =
        MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC);

    GeneratedCodePatterns.addFinalFieldAndConstructorArgument(
        ClassName.get(DefaultMapperContext.class), "context", classContents, constructorContents);

    // Add all the fields that were requested by DAO method generators:
    for (DaoSimpleField field : daoSimpleFields) {
      classContents.addField(
          FieldSpec.builder(
                  ParameterizedTypeName.get(ClassName.get(LazyReference.class), field.type),
                  field.name,
                  Modifier.PRIVATE,
                  Modifier.FINAL)
              .build());
      constructorContents.addStatement(
          "this.$1L = new $2T<>(() -> $3T.$4L(context))",
          field.name,
          LazyReference.class,
          field.daoImplementationType,
          field.isAsync ? "initAsync" : "init");
    }
    for (DaoMapField field : daoMapFields) {
      classContents.addField(
          FieldSpec.builder(
                  ParameterizedTypeName.get(
                      ClassName.get(ConcurrentMap.class),
                      TypeName.get(DaoCacheKey.class),
                      field.mapValueType),
                  field.name,
                  Modifier.PRIVATE,
                  Modifier.FINAL)
              .initializer("new $T<>()", ConcurrentHashMap.class)
              .build());
    }
    classContents.addMethod(constructorContents.build());

    return JavaFile.builder(className.packageName(), classContents.build());
  }

  private static class DaoSimpleField {
    final String name;
    final TypeName type;
    final TypeName daoImplementationType;
    final boolean isAsync;

    DaoSimpleField(String name, TypeName type, TypeName daoImplementationType, boolean isAsync) {
      this.name = name;
      this.type = type;
      this.daoImplementationType = daoImplementationType;
      this.isAsync = isAsync;
    }
  }

  private static class DaoMapField {
    final String name;
    final TypeName mapValueType;

    DaoMapField(String name, TypeName mapValueType) {
      this.name = name;
      this.mapValueType = mapValueType;
    }
  }
}
