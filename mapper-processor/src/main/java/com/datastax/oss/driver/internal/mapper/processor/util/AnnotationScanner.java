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
package com.datastax.oss.driver.internal.mapper.processor.util;

import java.lang.annotation.Annotation;
import java.util.Optional;
import java.util.Set;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

public class AnnotationScanner {
  public static <A extends Annotation> Optional<ResolvedAnnotation<A>> getClassAnnotation(
      Class<A> annotationType, Set<TypeElement> typeHierarchy) {
    for (TypeElement element : typeHierarchy) {
      A annotation = element.getAnnotation(annotationType);
      if (annotation != null) {
        return Optional.of(new ResolvedAnnotation<>(annotation, element));
      }
    }
    return Optional.empty();
  }

  public static <A extends Annotation> Optional<ResolvedAnnotation<A>> getMethodAnnotation(
      Class<A> annotationType, ExecutableElement getMethod, Set<TypeElement> typeHierarchy) {
    // first try evaluating the method as it is.
    A annotation = getMethod.getAnnotation(annotationType);
    if (annotation != null) {
      return Optional.of(new ResolvedAnnotation<>(annotation, getMethod));
    }

    // otherwise navigate the hierarchy until an annotation is found.
    for (TypeElement typeElement : typeHierarchy) {
      for (Element child : typeElement.getEnclosedElements()) {
        Set<Modifier> modifiers = child.getModifiers();
        if (child.getKind() != ElementKind.METHOD
            || modifiers.contains(Modifier.STATIC)
            || modifiers.contains(Modifier.PRIVATE)) {
          continue;
        }
        ExecutableElement candidateMethod = (ExecutableElement) child;
        TypeMirror typeMirror = candidateMethod.getReturnType();
        if (typeMirror.getKind() == TypeKind.VOID) {
          continue;
        }

        if (candidateMethod.getSimpleName().equals(getMethod.getSimpleName())) {
          annotation = candidateMethod.getAnnotation(annotationType);
          if (annotation != null) {
            return Optional.of(new ResolvedAnnotation<>(annotation, candidateMethod));
          }
        }
      }
    }
    return Optional.empty();
  }
}
