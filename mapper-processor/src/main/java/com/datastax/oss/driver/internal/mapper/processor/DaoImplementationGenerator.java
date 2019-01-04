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
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.SaveQuery;
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

/** Generates the implementation of a {@link Dao}-annotated interface. */
public class DaoImplementationGenerator extends FileGenerator {

  private final TypeElement interfaceElement;
  private final ClassName implementationName;

  public DaoImplementationGenerator(TypeElement interfaceElement, GenerationContext context) {
    super(context);
    this.interfaceElement = interfaceElement;
    implementationName =
        ClassName.get(interfaceElement).peerClass(interfaceElement.getSimpleName() + "_Impl");
  }

  @Override
  protected String getFileName() {
    return implementationName.packageName() + "." + implementationName.simpleName();
  }

  public ClassName getGeneratedClassName() {
    return implementationName;
  }

  @Override
  protected JavaFile.Builder getContents() {

    List<PartialClassGenerator> methods = new ArrayList<>();
    for (Element child : interfaceElement.getEnclosedElements()) {
      if (child.getKind() == ElementKind.METHOD) {
        ExecutableElement method = (ExecutableElement) child;
        try {
          SaveQuery saveQuery = method.getAnnotation(SaveQuery.class);
          if (saveQuery != null) {
            methods.add(new SaveQueryGenerator(method, saveQuery, context));
          }
          Query query = method.getAnnotation(Query.class);
          if (query != null) {
            methods.add(new QueryGenerator(method, query, context));
          }
        } catch (SkipGenerationException e) {
          // nothing to do (throwing code should have issued an error message already)
        }
      }
    }

    TypeSpec.Builder classBuilder =
        TypeSpec.classBuilder(implementationName)
            .addJavadoc(JAVADOC_GENERATED_WARNING)
            .addModifiers(Modifier.PUBLIC)
            .addSuperinterface(ClassName.get(interfaceElement))
            .addField(
                FieldSpec.builder(Session.class, "session", Modifier.PRIVATE, Modifier.FINAL)
                    .build());

    MethodSpec.Builder constructorBuilder =
        MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .addParameter(Session.class, "session")
            .addStatement("this.session = session");

    for (PartialClassGenerator method : methods) {
      method.addConstructorInstructions(constructorBuilder);
      method.addMembers(classBuilder);
    }

    classBuilder.addMethod(constructorBuilder.build());

    return JavaFile.builder(implementationName.packageName(), classBuilder.build());
  }
}
