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

import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.GetEntity;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.SetEntity;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.mapper.DaoBase;
import com.datastax.oss.driver.internal.mapper.MapperContext;
import com.datastax.oss.driver.internal.mapper.processor.GeneratedNames;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.SingleFileCodeGenerator;
import com.datastax.oss.driver.internal.mapper.processor.SkipGenerationException;
import com.datastax.oss.driver.internal.mapper.processor.util.NameIndex;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.BindableHandlingSharedCode;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.GenericTypeConstantGenerator;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import java.beans.Introspector;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;

public class DaoImplementationGenerator extends SingleFileCodeGenerator
    implements BindableHandlingSharedCode {

  private static final TypeName PREPARED_STATEMENT_STAGE =
      ParameterizedTypeName.get(CompletionStage.class, PreparedStatement.class);

  private final TypeElement interfaceElement;
  private final ClassName implementationName;
  private final NameIndex nameIndex = new NameIndex();
  private final GenericTypeConstantGenerator genericTypeConstantGenerator =
      new GenericTypeConstantGenerator(nameIndex);
  private final Map<ClassName, String> entityHelperFields = new LinkedHashMap<>();
  private final List<GeneratedPreparedStatement> preparedStatements = new ArrayList<>();

  public DaoImplementationGenerator(TypeElement interfaceElement, ProcessorContext context) {
    super(context);
    this.interfaceElement = interfaceElement;
    implementationName = GeneratedNames.daoImplementation(interfaceElement);
  }

  @Override
  public NameIndex getNameIndex() {
    return nameIndex;
  }

  @Override
  public String addGenericTypeConstant(TypeName type) {
    return genericTypeConstantGenerator.add(type);
  }

  @Override
  public String addEntityHelperField(ClassName entityClassName) {
    ClassName helperClass = GeneratedNames.entityHelper(entityClassName);
    return entityHelperFields.computeIfAbsent(
        helperClass,
        k -> {
          String baseName = Introspector.decapitalize(entityClassName.simpleName()) + "Helper";
          return nameIndex.uniqueField(baseName);
        });
  }

  /**
   * Requests the generation of a prepared statement in this DAO. It will be initialized in {@code
   * initAsync}, and then passed to the constructor which will store it in a private field.
   *
   * @param methodElement the method that will be using this statement.
   * @param simpleStatementGenerator a callback that generates code to create a {@link
   *     SimpleStatement} local variable that will be used to create the statement. The first
   *     parameter is the method to add to, and the second the name of the local variable.
   * @return the name of the generated field that will hold the statement.
   */
  String addPreparedStatement(
      ExecutableElement methodElement,
      BiConsumer<MethodSpec.Builder, String> simpleStatementGenerator) {
    // Prepared statements are not shared between methods, so always generate a new name
    String fieldName =
        nameIndex.uniqueField(methodElement.getSimpleName().toString() + "Statement");
    preparedStatements.add(
        new GeneratedPreparedStatement(methodElement, fieldName, simpleStatementGenerator));
    return fieldName;
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
          } else if (methodElement.getAnnotation(Insert.class) != null) {
            methods.add(new DaoInsertMethodGenerator(methodElement, this, context).generate());
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
            .superclass(DaoBase.class)
            .addSuperinterface(ClassName.get(interfaceElement));

    genericTypeConstantGenerator.generate(classBuilder);

    MethodSpec.Builder initAsyncBuilder = getInitAsyncContents();

    MethodSpec.Builder initBuilder = getInitContents();

    MethodSpec.Builder constructorBuilder =
        MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PRIVATE)
            .addParameter(MapperContext.class, "context")
            .addStatement("super(context)");

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

    // Same for prepared statements:
    for (GeneratedPreparedStatement preparedStatement : preparedStatements) {
      classBuilder.addField(
          FieldSpec.builder(
                  PreparedStatement.class,
                  preparedStatement.fieldName,
                  Modifier.PRIVATE,
                  Modifier.FINAL)
              .build());
      constructorBuilder
          .addParameter(PreparedStatement.class, preparedStatement.fieldName)
          .addStatement("this.$1L = $1L", preparedStatement.fieldName);
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
            .addParameter(MapperContext.class, "context")
            .beginControlFlow("try");

    // Start a constructor call: we build it dynamically because the number of parameters depends on
    // the entity helpers and prepared statements below.
    CodeBlock.Builder newDaoStatement = CodeBlock.builder();
    newDaoStatement.add("new $1T(context$>$>", implementationName);

    initAsyncBuilder.addComment("Initialize all entity helpers");
    // For each entity helper that was requested by a method generator:
    for (Map.Entry<ClassName, String> entry : entityHelperFields.entrySet()) {
      ClassName fieldTypeName = entry.getKey();
      String fieldName = entry.getValue();
      // - create an instance
      initAsyncBuilder.addStatement("$1T $2L = new $1T(context)", fieldTypeName, fieldName);
      // - add it as a parameter to the constructor call
      newDaoStatement.add(",\n$L", fieldName);
    }

    initAsyncBuilder.addStatement(
        "$T<$T> prepareStages = new $T<>()", List.class, PREPARED_STATEMENT_STAGE, ArrayList.class);
    // For each prepared statement that was requested by a method generator:
    for (GeneratedPreparedStatement preparedStatement : preparedStatements) {
      initAsyncBuilder.addComment(
          "Prepare the statement for `$L`:", preparedStatement.methodElement.toString());
      // - generate the simple statement
      String simpleStatementName = preparedStatement.fieldName + "_simple";
      preparedStatement.simpleStatementGenerator.accept(initAsyncBuilder, simpleStatementName);
      // - prepare it asynchronously, store all CompletionStages in a list
      initAsyncBuilder
          .addStatement(
              "$T $L = prepare($L, context)",
              PREPARED_STATEMENT_STAGE,
              preparedStatement.fieldName,
              simpleStatementName)
          .addStatement("prepareStages.add($L)", preparedStatement.fieldName);
      // - add the stage's result to the constructor call (which will be executed once all stages
      //   are complete)
      newDaoStatement.add(
          ",\n$T.getCompleted($L)", CompletableFutures.class, preparedStatement.fieldName);
    }

    newDaoStatement.add(")");

    initAsyncBuilder
        .addComment("Build the DAO when all statements are prepared")
        .addCode("$[return $T.allSuccessful(prepareStages)", CompletableFutures.class)
        .addCode("\n.thenApply(v -> ($T) ", interfaceElement)
        .addCode(newDaoStatement.build())
        .addCode(")\n$<$<.toCompletableFuture();$]\n")
        .nextControlFlow("catch ($T t)", Throwable.class)
        .addStatement("return $T.failedFuture(t)", CompletableFutures.class)
        .endControlFlow();
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

  private static class GeneratedPreparedStatement {
    final ExecutableElement methodElement;
    final String fieldName;
    final BiConsumer<MethodSpec.Builder, String> simpleStatementGenerator;

    GeneratedPreparedStatement(
        ExecutableElement methodElement,
        String fieldName,
        BiConsumer<MethodSpec.Builder, String> simpleStatementGenerator) {
      this.methodElement = methodElement;
      this.fieldName = fieldName;
      this.simpleStatementGenerator = simpleStatementGenerator;
    }
  }
}
