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
package com.datastax.oss.driver.internal.mapper.processor.entity;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.internal.mapper.MapperContext;
import com.datastax.oss.driver.internal.mapper.processor.GeneratedNames;
import com.datastax.oss.driver.internal.mapper.processor.PartialClassGenerator;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.SingleFileCodeGenerator;
import com.datastax.oss.driver.internal.mapper.processor.util.NameIndex;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.BindableHandlingSharedCode;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.GenericTypeConstantGenerator;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import java.beans.Introspector;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;

public class EntityHelperGenerator extends SingleFileCodeGenerator
    implements BindableHandlingSharedCode {

  private final TypeElement classElement;
  private final EntityDefinition entityDefinition;
  private final ClassName helperName;
  private final NameIndex nameIndex = new NameIndex();
  private final GenericTypeConstantGenerator genericTypeConstantGenerator =
      new GenericTypeConstantGenerator(nameIndex);
  private final Map<ClassName, String> childHelpers = new HashMap<>();

  public EntityHelperGenerator(TypeElement classElement, ProcessorContext context) {
    super(context);
    this.classElement = classElement;
    entityDefinition = context.getEntityFactory().getDefinition(classElement);
    helperName = GeneratedNames.entityHelper(classElement);
  }

  @Override
  public NameIndex getNameIndex() {
    return nameIndex;
  }

  @Override
  protected String getFileName() {
    return helperName.packageName() + "." + helperName.simpleName();
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
          String baseName = Introspector.decapitalize(childEntityName.simpleName()) + "Helper";
          return nameIndex.uniqueField(baseName);
        });
  }

  @Override
  protected JavaFile.Builder getContents() {

    List<PartialClassGenerator> methodGenerators =
        ImmutableList.of(
            new EntityHelperSetMethodGenerator(entityDefinition, this, context),
            new EntityHelperGetMethodGenerator(entityDefinition, this, context),
            new EntityHelperInsertMethodGenerator(entityDefinition, this, context));

    TypeSpec.Builder classContents =
        TypeSpec.classBuilder(helperName)
            .addJavadoc(JAVADOC_GENERATED_WARNING)
            .addModifiers(Modifier.PUBLIC)
            .addSuperinterface(
                ParameterizedTypeName.get(
                    ClassName.get(EntityHelper.class), ClassName.get(classElement)))
            .addField(
                FieldSpec.builder(MapperContext.class, "context", Modifier.PRIVATE, Modifier.FINAL)
                    .build())
            .addField(
                FieldSpec.builder(
                        CqlIdentifier.class, "defaultTableId", Modifier.PRIVATE, Modifier.FINAL)
                    .initializer(
                        "$T.fromCql($S)", CqlIdentifier.class, entityDefinition.getCqlName())
                    .build());

    MethodSpec.Builder constructorContents =
        MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .addParameter(MapperContext.class, "context")
            .addStatement("this.context = context");

    for (PartialClassGenerator methodGenerator : methodGenerators) {
      methodGenerator.addConstructorInstructions(constructorContents);
      methodGenerator.addMembers(classContents);
    }

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
