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

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.mapper.annotations.SaveQuery;
import com.datastax.oss.driver.api.mapper.annotations.Table;
import com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl;
import com.datastax.oss.driver.internal.core.cql.DefaultPrepareRequest;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import java.util.List;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;

public class SaveQueryGenerator implements DaoMethodGenerator {

  private final String methodName;
  private final String parameterName;
  private final ClassName parameterType;
  private final String preparedStatementFieldName;
  private final String tableName;
  private final EntityDefinition entityDefinition;

  public SaveQueryGenerator(
      ExecutableElement methodElement, SaveQuery annotation, GenerationContext context) {
    this.methodName = methodElement.getSimpleName().toString();
    preparedStatementFieldName = methodName + "_preparedStatement";
    tableName = annotation.table();
    List<? extends VariableElement> parameters = methodElement.getParameters();
    if (parameters.size() != 1) {
      context.getMessager().error(methodElement, "Expected a single argument");
      throw new SkipGenerationException();
    }
    VariableElement parameter = parameters.get(0);
    parameterName = parameter.getSimpleName().toString();
    parameterType = getType(parameter, context);
    entityDefinition = context.getEntityDefinitions().get(parameterType);
    if (entityDefinition == null) {
      context
          .getMessager()
          .error(methodElement, "Could not find definition of entity %s", parameterType);
      throw new SkipGenerationException();
    }
  }

  // Get the type of the given parameter, expecting a non-generic class
  private static ClassName getType(VariableElement parameter, GenerationContext context) {
    TypeMirror mirror = parameter.asType();
    Element element = context.getTypeUtils().asElement(mirror);
    if (element == null || element.getKind() != ElementKind.CLASS) {
      context.getMessager().error(parameter, "Expected entity class, got %s", mirror);
      throw new SkipGenerationException();
    }
    TypeElement typeElement = (TypeElement) element;
    if (typeElement.getAnnotation(Table.class) == null) {
      context
          .getMessager()
          .error(
              parameter,
              "Should be an instance of a class annotated with %s",
              Table.class.getSimpleName());
    }
    return ClassName.get(typeElement);
  }

  @Override
  public void addMembers(TypeSpec.Builder classBuilder) {
    addPreparedStatementField(classBuilder);
    addMethodImplementation(classBuilder);
  }

  private void addPreparedStatementField(TypeSpec.Builder classBuilder) {
    classBuilder.addField(
        FieldSpec.builder(
                PreparedStatement.class,
                preparedStatementFieldName,
                Modifier.PRIVATE,
                Modifier.FINAL)
            .build());
  }

  private void addMethodImplementation(TypeSpec.Builder classBuilder) {
    MethodSpec.Builder methodBuilder =
        MethodSpec.methodBuilder(methodName)
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(parameterType, parameterName)
            .addStatement(
                "$T boundStatement = $L.bind()", BoundStatement.class, preparedStatementFieldName);

    for (FieldDefinition field : entityDefinition) {
      methodBuilder.addStatement(
          "boundStatement.set($S, $L.$L(), $T.class)",
          field.getColumnName(),
          parameterName,
          field.getGetterName(),
          field.getType());
    }
    methodBuilder.addStatement("session.execute(boundStatement, $T.SYNC)", Statement.class);

    classBuilder.addMethod(methodBuilder.build());
  }

  @Override
  public void addConstructorInstructions(MethodSpec.Builder constructorBuilder) {
    String queryName = methodName + "Query";
    constructorBuilder.addCode(
        "$[String $L = $T.insertInto($S)", queryName, QueryBuilderDsl.class, tableName);
    for (FieldDefinition field : entityDefinition) {
      constructorBuilder.addCode(
          "\n.value($S, $T.bindMarker())", field.getColumnName(), QueryBuilderDsl.class);
    }
    constructorBuilder.addCode("\n.asCql();$]\n");

    constructorBuilder.addStatement(
        "this.$L = session.execute(new $T($L), $T.SYNC)",
        preparedStatementFieldName,
        DefaultPrepareRequest.class,
        queryName,
        PrepareRequest.class);
  }
}
