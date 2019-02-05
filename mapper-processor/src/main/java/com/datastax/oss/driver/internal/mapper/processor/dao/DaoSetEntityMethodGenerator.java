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
package com.datastax.oss.driver.internal.mapper.processor.dao;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.SetEntity;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.SkipGenerationException;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

public class DaoSetEntityMethodGenerator implements MethodGenerator {

  private final ExecutableElement methodElement;
  private final DaoImplementationGenerator daoImplementationGenerator;
  private final String entityParameterName;
  private final TypeElement entityElement;
  private final String targetParameterName;
  private final boolean isVoid;

  public DaoSetEntityMethodGenerator(
      ExecutableElement methodElement,
      DaoImplementationGenerator daoImplementationGenerator,
      ProcessorContext context) {
    this.methodElement = methodElement;
    this.daoImplementationGenerator = daoImplementationGenerator;
    // We're expecting a method where one parameter is an annotated entity, and the other a
    // SettableByName or subtype. It can either be void or return the SettableByName.
    if (methodElement.getParameters().size() != 2) {
      context
          .getMessager()
          .error(
              methodElement,
              "%s methods must have two parameters",
              SetEntity.class.getSimpleName());
      throw new SkipGenerationException();
    }
    String tmpEntity = null;
    TypeElement tmpEntityElement = null;
    String tmpTarget = null;
    TypeMirror targetParameterType = null;
    for (VariableElement parameterElement : methodElement.getParameters()) {
      TypeMirror parameterType = parameterElement.asType();
      if (context.getClassUtils().implementsSettableByName(parameterType)) {
        tmpTarget = parameterElement.getSimpleName().toString();
        targetParameterType = parameterElement.asType();
      } else if (parameterType.getKind() == TypeKind.DECLARED) {
        Element parameterTypeElement = ((DeclaredType) parameterType).asElement();
        if (parameterTypeElement.getKind() == ElementKind.CLASS
            && parameterTypeElement.getAnnotation(Entity.class) != null) {
          tmpEntity = parameterElement.getSimpleName().toString();
          tmpEntityElement = ((TypeElement) parameterTypeElement);
        }
      }
    }
    if (tmpEntity == null || tmpTarget == null) {
      context
          .getMessager()
          .error(
              methodElement,
              "Could not match parameters, expected a SettableByName "
                  + "and an annotated entity (in any order)");
      throw new SkipGenerationException();
    }
    this.entityParameterName = tmpEntity;
    this.entityElement = tmpEntityElement;
    this.targetParameterName = tmpTarget;
    TypeMirror returnType = methodElement.getReturnType();
    this.isVoid = returnType.getKind() == TypeKind.VOID;
    if (isVoid) {
      if (context.getClassUtils().isSame(targetParameterType, BoundStatement.class)) {
        context
            .getMessager()
            .warn(
                methodElement,
                "BoundStatement is immutable, "
                    + "this method will not modify '%s' in place. "
                    + "It should probably return BoundStatement rather than void",
                targetParameterName);
      }
    } else if (!context.getTypeUtils().isSameType(returnType, targetParameterType)) {
      context
          .getMessager()
          .error(
              methodElement,
              "Invalid return type, should be the same as '%s' (%s)",
              targetParameterName,
              targetParameterType);
    }
  }

  @Override
  public MethodSpec.Builder generate() {
    String helperFieldName =
        daoImplementationGenerator.addEntityHelperField(ClassName.get(entityElement));

    MethodSpec.Builder overridingMethodBuilder =
        MethodSpec.methodBuilder(methodElement.getSimpleName().toString())
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ClassName.get(methodElement.getReturnType()));
    for (VariableElement parameterElement : methodElement.getParameters()) {
      overridingMethodBuilder.addParameter(
          ClassName.get(parameterElement.asType()), parameterElement.getSimpleName().toString());
    }
    // Forward to the base injector in the helper:
    overridingMethodBuilder.addStatement(
        "$L$L.set($L, $L)",
        isVoid ? "" : "return ",
        helperFieldName,
        entityParameterName,
        targetParameterName);

    return overridingMethodBuilder;
  }
}
