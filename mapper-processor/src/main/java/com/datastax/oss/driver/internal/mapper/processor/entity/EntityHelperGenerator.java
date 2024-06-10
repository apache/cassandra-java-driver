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
package com.datastax.oss.driver.internal.mapper.processor.entity;

import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.internal.mapper.entity.EntityHelperBase;
import com.datastax.oss.driver.internal.mapper.processor.GeneratedNames;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.SingleFileCodeGenerator;
import com.datastax.oss.driver.internal.mapper.processor.util.Capitalizer;
import com.datastax.oss.driver.internal.mapper.processor.util.NameIndex;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.BindableHandlingSharedCode;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.GenericTypeConstantGenerator;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;

public class EntityHelperGenerator extends SingleFileCodeGenerator
    implements BindableHandlingSharedCode {

  private final TypeElement classElement;
  private final ClassName helperName;
  private final NameIndex nameIndex = new NameIndex();
  private final GenericTypeConstantGenerator genericTypeConstantGenerator =
      new GenericTypeConstantGenerator(nameIndex);
  private final Map<ClassName, String> childHelpers = new HashMap<>();

  public EntityHelperGenerator(TypeElement classElement, ProcessorContext context) {
    super(context);
    this.classElement = classElement;
    helperName = GeneratedNames.entityHelper(classElement);
  }

  @Override
  public NameIndex getNameIndex() {
    return nameIndex;
  }

  @Override
  protected ClassName getPrincipalTypeName() {
    return helperName;
  }

  @Override
  public String addGenericTypeConstant(TypeName type) {
    return genericTypeConstantGenerator.add(type);
  }

  @Override
  public String addEntityHelperField(ClassName childEntityName) {
    return childHelpers.computeIfAbsent(
        childEntityName,
        k -> {
          String baseName = Capitalizer.decapitalize(childEntityName.simpleName()) + "Helper";
          return nameIndex.uniqueField(baseName);
        });
  }

  @Override
  protected JavaFile.Builder getContents() {
    EntityDefinition entityDefinition = context.getEntityFactory().getDefinition(classElement);
    TypeSpec.Builder classContents =
        TypeSpec.classBuilder(helperName)
            .addJavadoc(JAVADOC_GENERATED_WARNING)
            .addAnnotation(
                AnnotationSpec.builder(SuppressWarnings.class)
                    .addMember("value", "\"all\"")
                    .build())
            .addModifiers(Modifier.PUBLIC)
            .superclass(
                ParameterizedTypeName.get(
                    ClassName.get(EntityHelperBase.class), ClassName.get(classElement)));

    context.getLoggingGenerator().addLoggerField(classContents, helperName);

    classContents.addMethod(
        MethodSpec.methodBuilder("getEntityClass")
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(Override.class)
            .returns(
                ParameterizedTypeName.get(
                    ClassName.get(Class.class), entityDefinition.getClassName()))
            .addStatement("return $T.class", entityDefinition.getClassName())
            .build());

    for (MethodGenerator methodGenerator :
        ImmutableList.of(
            new EntityHelperSetMethodGenerator(entityDefinition, this),
            new EntityHelperGetMethodGenerator(entityDefinition, this),
            new EntityHelperInsertMethodGenerator(entityDefinition),
            new EntityHelperSelectByPrimaryKeyPartsMethodGenerator(),
            new EntityHelperSelectByPrimaryKeyMethodGenerator(),
            new EntityHelperSelectStartMethodGenerator(entityDefinition),
            new EntityHelperDeleteStartMethodGenerator(),
            new EntityHelperDeleteByPrimaryKeyPartsMethodGenerator(entityDefinition),
            new EntityHelperDeleteByPrimaryKeyMethodGenerator(),
            new EntityHelperUpdateStartMethodGenerator(entityDefinition),
            new EntityHelperUpdateByPrimaryKeyMethodGenerator(entityDefinition),
            new EntityHelperSchemaValidationMethodGenerator(
                entityDefinition, classElement, context.getLoggingGenerator(), this))) {
      methodGenerator.generate().ifPresent(classContents::addMethod);
    }

    MethodSpec.Builder constructorContents =
        MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC);

    constructorContents.addParameter(ClassName.get(MapperContext.class), "context");

    if (entityDefinition.getDefaultKeyspace() == null) {
      constructorContents.addStatement("super(context, $L)", entityDefinition.getCqlName());
    } else {
      constructorContents.addStatement(
          "super(context, $S, $L)",
          entityDefinition.getDefaultKeyspace(),
          entityDefinition.getCqlName());
    }
    context
        .getLoggingGenerator()
        .debug(
            constructorContents,
            String.format(
                "[{}] Entity %s will be mapped to {}{}",
                entityDefinition.getClassName().simpleName()),
            CodeBlock.of("context.getSession().getName()"),
            CodeBlock.of("getKeyspaceId() == null ? \"\" : getKeyspaceId() + \".\""),
            CodeBlock.of("getTableId()"));

    // retain primary keys for reference in methods.
    classContents.addField(
        FieldSpec.builder(
                ParameterizedTypeName.get(List.class, String.class),
                "primaryKeys",
                Modifier.PRIVATE,
                Modifier.FINAL)
            .build());

    constructorContents.addCode(
        "$[this.primaryKeys = $1T.<$2T>builder()", ImmutableList.class, String.class);
    for (PropertyDefinition propertyDefinition : entityDefinition.getPrimaryKey()) {
      constructorContents.addCode("\n.add($1L)", propertyDefinition.getCqlName());
    }

    constructorContents.addCode("\n.build()$];\n");

    genericTypeConstantGenerator.generate(classContents);

    for (Map.Entry<ClassName, String> entry : childHelpers.entrySet()) {
      ClassName childEntityName = entry.getKey();
      String fieldName = entry.getValue();

      ClassName helperClassName = GeneratedNames.entityHelper(childEntityName);
      classContents.addField(
          FieldSpec.builder(helperClassName, fieldName, Modifier.PRIVATE, Modifier.FINAL).build());

      constructorContents.addStatement("this.$L = new $T(context)", fieldName, helperClassName);
    }

    classContents.addMethod(constructorContents.build());

    return JavaFile.builder(helperName.packageName(), classContents.build());
  }
}
