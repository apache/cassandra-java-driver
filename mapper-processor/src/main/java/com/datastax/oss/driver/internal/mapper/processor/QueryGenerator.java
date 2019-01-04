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
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.internal.core.cql.DefaultPrepareRequest;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import java.util.ArrayList;
import java.util.List;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

public class QueryGenerator implements PartialClassGenerator {

  private final ExecutableElement methodElement;
  private final String preparedStatementFieldName;
  private final String queryString;
  private final GenerationContext context;

  public QueryGenerator(
      ExecutableElement methodElement, Query annotation, GenerationContext context) {
    this.methodElement = methodElement;
    preparedStatementFieldName = methodElement.getSimpleName() + "_preparedStatement";
    queryString = annotation.value();
    this.context = context;
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
    if (methodElement.getReturnType().getKind() != TypeKind.DECLARED) {
      // TODO is this necessary? can't have anything else as a return type
      context.getMessager().error(methodElement, "Expected a declared type as return type");
      throw new SkipGenerationException();
    }
    DeclaredType returnType = (DeclaredType) methodElement.getReturnType();

    // TODO handle parameters if applicable (add them to signature, bind them...)
    MethodSpec.Builder methodBuilder =
        MethodSpec.methodBuilder(methodElement.getSimpleName().toString())
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ClassName.get(returnType))
            .addStatement(
                "$T boundStatement = $L.bind()", BoundStatement.class, preparedStatementFieldName)
            .addStatement(
                "$T resultSet = session.execute(boundStatement, $T.SYNC)",
                ResultSet.class,
                Statement.class);

    if (isList(returnType)) {
      TypeMirror elementType = returnType.getTypeArguments().get(0);
      EntityDefinition entityDefinition = context.getEntityDefinitions().get(elementType);
      if (entityDefinition == null) {
        context.getMessager().error(methodElement, "Missing definition for %s", elementType);
        throw new SkipGenerationException();
      }

      methodBuilder.addStatement(
          "$T<$T> result = new $T<>()", List.class, elementType, ArrayList.class);
      methodBuilder.beginControlFlow("for ($T row : resultSet)", Row.class);

      methodBuilder.addStatement("$1T element = new $1T()", elementType);
      for (FieldDefinition field : entityDefinition) {
        methodBuilder.addStatement(
            "element.$L(row.get($S, $T.class))",
            field.getSetterName(),
            field.getColumnName(),
            field.getType());
      }
      methodBuilder.addStatement("result.add(element)");

      methodBuilder.endControlFlow();
      methodBuilder.addStatement("return result");
    } else {
      // TODO handle other result types
    }

    classBuilder.addMethod(methodBuilder.build());
  }

  private boolean isList(DeclaredType type) {
    TypeMirror list =
        context.getElementUtils().getTypeElement(List.class.getCanonicalName()).asType();
    TypeMirror erased = context.getTypeUtils().erasure(type);
    return context.getTypeUtils().isAssignable(list, erased);
  }

  @Override
  public void addConstructorInstructions(MethodSpec.Builder constructorBuilder) {
    constructorBuilder.addStatement(
        "this.$L = session.execute(new $T($S), $T.SYNC)",
        preparedStatementFieldName,
        DefaultPrepareRequest.class,
        queryString,
        PrepareRequest.class);
  }
}
