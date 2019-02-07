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

import com.datastax.oss.driver.api.mapper.annotations.GetEntity;
import com.datastax.oss.driver.api.mapper.annotations.SetEntity;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.mapper.MapperContext;
import com.datastax.oss.driver.internal.mapper.processor.GeneratedNames;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.SingleFileCodeGenerator;
import com.datastax.oss.driver.internal.mapper.processor.SkipGenerationException;
import com.datastax.oss.driver.internal.mapper.processor.util.NameIndex;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import java.beans.Introspector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;

public class DaoImplementationGenerator extends SingleFileCodeGenerator {

  private final TypeElement interfaceElement;
  private final ClassName implementationName;
  private final NameIndex nameIndex = new NameIndex();
  private final Map<ClassName, String> entityHelperFields = new HashMap<>();

  public DaoImplementationGenerator(TypeElement interfaceElement, ProcessorContext context) {
    super(context);
    this.interfaceElement = interfaceElement;
    implementationName = GeneratedNames.daoImplementation(interfaceElement);
  }

  /**
   * Requests the generation of a field holding the {@link EntityHelper} that was generated for the
   * given entity class, along with the initialization code in the constructor.
   *
   * <p>If this is called multiple times, only a single field will be created.
   *
   * @return the name of the field.
   */
  String addEntityHelperField(TypeElement entityClass) {
    ClassName helperClass = GeneratedNames.entityHelper(entityClass);
    return entityHelperFields.computeIfAbsent(
        helperClass,
        k -> {
          String baseName =
              Introspector.decapitalize(entityClass.getSimpleName().toString()) + "Helper";
          return nameIndex.uniqueField(baseName);
        });
  }

  @Override
  protected String getFileName() {
    return implementationName.packageName() + "." + implementationName.simpleName();
  }

  @Override
  protected JavaFile.Builder getContents() {

    List<MethodSpec.Builder> methods = new ArrayList<>();
    for (Element child : interfaceElement.getEnclosedElements()) {
      try {
        if (child.getKind() == ElementKind.METHOD) {
          ExecutableElement methodElement = (ExecutableElement) child;
          if (methodElement.getAnnotation(SetEntity.class) != null) {
            methods.add(new DaoSetEntityMethodGenerator(methodElement, this, context).generate());
          }
          if (methodElement.getAnnotation(GetEntity.class) != null) {
            methods.add(new DaoGetEntityMethodGenerator(methodElement, this, context).generate());
          }
          // TODO handle other annotations
        }
      } catch (SkipGenerationException ignored) {
      }
    }

    TypeSpec.Builder classBuilder =
        TypeSpec.classBuilder(implementationName)
            .addJavadoc(JAVADOC_GENERATED_WARNING)
            .addModifiers(Modifier.PUBLIC)
            .addSuperinterface(ClassName.get(interfaceElement))
            .addField(
                FieldSpec.builder(MapperContext.class, "context", Modifier.PRIVATE, Modifier.FINAL)
                    .build());

    MethodSpec.Builder initAsyncBuilder = getInitAsyncContents();

    MethodSpec.Builder initBuilder = getInitContents();

    MethodSpec.Builder constructorBuilder =
        MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PRIVATE)
            .addParameter(MapperContext.class, "context")
            .addStatement("this.context = context");

    // For each entity helper that was requested by a method generator, create a field for it and
    // add a constructor parameter for it (the instance gets created in initAsync).
    for (Map.Entry<ClassName, String> entry : entityHelperFields.entrySet()) {
      ClassName fieldTypeName = entry.getKey();
      String fieldName = entry.getValue();

      classBuilder.addField(
          FieldSpec.builder(fieldTypeName, fieldName, Modifier.PRIVATE, Modifier.FINAL).build());
      constructorBuilder
          .addParameter(fieldTypeName, fieldName)
          .addStatement("this.$1L = $1L", fieldName);
    }

    classBuilder.addMethod(initAsyncBuilder.build());
    classBuilder.addMethod(initBuilder.build());
    classBuilder.addMethod(constructorBuilder.build());

    for (MethodSpec.Builder method : methods) {
      classBuilder.addMethod(method.build());
    }

    return JavaFile.builder(implementationName.packageName(), classBuilder.build());
  }

  /**
   * Generates the DAO's initAsync() builder: this is the entry point, that the main mapper will use
   * to build instances.
   *
   * <p>In this method we want to instantiate any entity helper or prepared statement that will be
   * needed by methods of the DAO. Then we call the DAO's private constructor, passing that
   * information.
   */
  private MethodSpec.Builder getInitAsyncContents() {
    MethodSpec.Builder initAsyncBuilder =
        MethodSpec.methodBuilder("initAsync")
            .returns(
                ParameterizedTypeName.get(
                    ClassName.get(CompletableFuture.class), ClassName.get(interfaceElement)))
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .addParameter(MapperContext.class, "context");

    // Start a constructor call: we build it dynamically because the number of parameters depends on
    // the entity helpers and prepared statements below.
    CodeBlock.Builder newDaoStatement = CodeBlock.builder();
    newDaoStatement.add("$[$1T dao = new $1T(context", implementationName);

    // For each entity helper that was requested by a method generator:
    // - create an instance
    // - add it as a parameter to the constructor call
    // Example:
    // User_Helper userHelper = new User_Helper(context);
    // Address_Helper addressHelper = new Address_Helper(context);
    // UserDao_Impl dao = new UserDao_Impl(context,
    //     userHelper,
    //     addressHelper);
    for (Map.Entry<ClassName, String> entry : entityHelperFields.entrySet()) {
      ClassName fieldTypeName = entry.getKey();
      String fieldName = entry.getValue();
      initAsyncBuilder.addStatement("$1T $2L = new $1T(context)", fieldTypeName, fieldName);
      newDaoStatement.add(",\n$L", fieldName);
    }
    newDaoStatement.add(")$];\n");

    // Finally emit the complete constructor call, and wrap the result in a future
    // TODO this will change once we also create prepared statements in this method
    initAsyncBuilder.addCode(newDaoStatement.build());
    initAsyncBuilder.addStatement("return $T.completedFuture(dao)", CompletableFuture.class);
    return initAsyncBuilder;
  }

  /** Generates the DAO's init() method: it's a simple synchronous wrapper of initAsync(). */
  private MethodSpec.Builder getInitContents() {
    return MethodSpec.methodBuilder("init")
        .returns(ClassName.get(interfaceElement))
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .addParameter(MapperContext.class, "context")
        .addStatement("$T.checkNotDriverThread()", BlockingOperation.class)
        .addStatement("return $T.getUninterruptibly(initAsync(context))", CompletableFutures.class);
  }
}
