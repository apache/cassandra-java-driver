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

import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.SetEntity;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.util.Classes;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;

public class NullSavingStrategyValidation {
  private final Classes classUtils;

  public NullSavingStrategyValidation(ProcessorContext context) {
    classUtils = context.getClassUtils();
  }

  /**
   * For every ExecutableElement that has @{@link NullSavingStrategy} property checks if {@link
   * NullSavingStrategy#DO_NOT_SET} was set. The underlying annotations that have that strategy are:
   * {@link Update#nullSavingStrategy()}, {@link Insert#nullSavingStrategy()}, {@link
   * SetEntity#nullSavingStrategy()} {@link Query#nullSavingStrategy()}
   *
   * @return true if:
   *     <p>DAO level has SET_TO_NULL and any of underlying has explicit set to DO_NOT_SET
   *     <p>DAO level has DO_NOT_SET and not all underlying override it explicitly to SET_TO_NULL
   *     <p>DAO level annotation do not present and any of method level strategy has DO_NOT_SET
   *     (including the default one)
   */
  public boolean hasDoNotSetOnAnyLevel(
      List<ExecutableElement> methodElements, @Nullable DefaultNullSavingStrategy annotation) {
    boolean anyMethodHasOrDefaultsToDoNotSet =
        methodElements.stream()
            .anyMatch(
                v ->
                    updateHasDoNotSet(v, false)
                        || insertHasDoNotSet(v, false)
                        || setEntityHasDoNotSet(v, false)
                        || queryHasDoNotSet(v, false));

    boolean anyMethodHasDoNotSetExplicitly =
        methodElements.stream()
            .anyMatch(
                v ->
                    updateHasDoNotSet(v, true)
                        || insertHasDoNotSet(v, true)
                        || setEntityHasDoNotSet(v, true)
                        || queryHasDoNotSet(v, true));

    boolean allMethodsHaveSetToNull =
        methodElements.stream()
            .filter(this::isOperationWithNullSavingStrategy)
            .allMatch(
                v ->
                    updateHasSetToNullExplicitly(v)
                        || insertHasSetToNullExplicitly(v)
                        || setEntitySetToNullExplicitly(v)
                        || queryHasSetToNullExplicitly(v));

    // if DAO level SET_TO_NULL check all underlying annotations for explicit set to DO_NOT_SET
    // (they may override it)
    if (daoHasSetToNull(annotation) && anyMethodHasDoNotSetExplicitly) {
      return true;
      // if DAO level DO_NOT_SET check if all underlying override it explicitly to SET_TO_NULL
    } else if (daoHasDoNotSet(annotation) && !allMethodsHaveSetToNull) {
      return true;
      // if DAO level annotation do not present, check method level strategy
      // (including the default one)
    } else {
      return daoIsNotAnnotated(annotation) && anyMethodHasOrDefaultsToDoNotSet;
    }
  }

  private boolean daoHasDoNotSet(DefaultNullSavingStrategy annotation) {
    if (annotation != null) {
      return annotation.value() == NullSavingStrategy.DO_NOT_SET;
    }
    return false;
  }

  private boolean daoHasSetToNull(DefaultNullSavingStrategy annotation) {
    if (annotation != null) {
      return annotation.value() == NullSavingStrategy.SET_TO_NULL;
    }
    return false;
  }

  private boolean daoIsNotAnnotated(DefaultNullSavingStrategy annotation) {
    return annotation == null;
  }

  private boolean queryHasDoNotSet(ExecutableElement v, boolean explicitSet) {
    return hasDoNotSet(Query.class, Query::nullSavingStrategy, v, explicitSet);
  }

  private boolean setEntityHasDoNotSet(ExecutableElement v, boolean explicitSet) {
    return hasDoNotSet(SetEntity.class, SetEntity::nullSavingStrategy, v, explicitSet);
  }

  private boolean insertHasDoNotSet(ExecutableElement v, boolean explicitSet) {
    return hasDoNotSet(Insert.class, Insert::nullSavingStrategy, v, explicitSet);
  }

  private boolean updateHasDoNotSet(ExecutableElement v, boolean explicitSet) {
    return hasDoNotSet(Update.class, Update::nullSavingStrategy, v, explicitSet);
  }

  private <A extends Annotation> boolean hasDoNotSet(
      Class<A> clazz,
      Function<A, NullSavingStrategy> extractor,
      ExecutableElement v,
      boolean explicitSet) {
    A annotation = v.getAnnotation(clazz);
    if (annotation != null) {
      NullSavingStrategy strategy = extractor.apply(annotation);
      if (explicitSet) {
        return strategy == NullSavingStrategy.DO_NOT_SET
            && nullSavingStrategyExplicitlySet(v, clazz);
      } else {
        return strategy == NullSavingStrategy.DO_NOT_SET;
      }
    }
    return false;
  }

  private boolean queryHasSetToNullExplicitly(ExecutableElement v) {
    return hadSetToNullExplicitly(Query.class, Query::nullSavingStrategy, v);
  }

  private boolean setEntitySetToNullExplicitly(ExecutableElement v) {
    return hadSetToNullExplicitly(SetEntity.class, SetEntity::nullSavingStrategy, v);
  }

  private boolean insertHasSetToNullExplicitly(ExecutableElement v) {
    return hadSetToNullExplicitly(Insert.class, Insert::nullSavingStrategy, v);
  }

  private boolean updateHasSetToNullExplicitly(ExecutableElement v) {
    return hadSetToNullExplicitly(Update.class, Update::nullSavingStrategy, v);
  }

  private <A extends Annotation> boolean hadSetToNullExplicitly(
      Class<A> clazz, Function<A, NullSavingStrategy> extractor, ExecutableElement v) {
    A annotation = v.getAnnotation(clazz);
    if (annotation != null) {
      NullSavingStrategy strategy = extractor.apply(annotation);
      return strategy == NullSavingStrategy.SET_TO_NULL
          && nullSavingStrategyExplicitlySet(v, clazz);
    } else {
      return false;
    }
  }

  private boolean isOperationWithNullSavingStrategy(ExecutableElement v) {
    return v.getAnnotation(Update.class) != null
        || v.getAnnotation(Insert.class) != null
        || v.getAnnotation(SetEntity.class) != null
        || v.getAnnotation(Query.class) != null;
  }

  private boolean nullSavingStrategyExplicitlySet(Element methodElement, Class<?> javaClass) {
    Optional<AnnotationMirror> annotationMirrorForJavaClass =
        getAnnotationMirrorForJavaClass(methodElement, javaClass);
    // Find out if NullSavingStrategy was set explicitly
    if (annotationMirrorForJavaClass.isPresent()) {
      for (ExecutableElement executableElement :
          annotationMirrorForJavaClass.get().getElementValues().keySet()) {
        if (executableElement.getSimpleName().contentEquals("nullSavingStrategy")) {
          return true;
        }
      }
    }
    return false;
  }

  private Optional<AnnotationMirror> getAnnotationMirrorForJavaClass(
      Element methodElement, Class<?> javaClass) {
    List<? extends AnnotationMirror> annotationMirrors = methodElement.getAnnotationMirrors();
    for (AnnotationMirror annotationMirror : annotationMirrors) {
      if (classUtils.isSame(annotationMirror.getAnnotationType(), javaClass)) {
        return Optional.of(annotationMirror);
      }
    }
    return Optional.empty();
  }

  public <A extends Annotation> NullSavingStrategy getNullSavingStrategy(
      Class<A> clazz,
      Function<A, NullSavingStrategy> extractor,
      ExecutableElement methodElement,
      DaoImplementationSharedCode daoClass) {
    A annotation = methodElement.getAnnotation(clazz);
    Optional<NullSavingStrategy> daoNullSavingStrategy = daoClass.getNullSavingStrategy();
    boolean methodNullSavingStrategyExplicitlySet =
        nullSavingStrategyExplicitlySet(methodElement, clazz);
    // Take method level strategy when explicitly set OR dao level default not specified
    if (methodNullSavingStrategyExplicitlySet || !daoNullSavingStrategy.isPresent()) {
      return extractor.apply(annotation);
    } else {
      // Take default when method level not specified and DAO level default present
      return daoNullSavingStrategy.get();
    }
  }
}
