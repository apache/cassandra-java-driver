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
package com.datastax.oss.driver.internal.mapper.processor;

import edu.umd.cs.findbugs.annotations.NonNull;
import javax.annotation.processing.Messager;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;

/** Wraps {@link Messager} to provide convenience methods. */
public class DecoratedMessager {

  private final Messager messager;

  public DecoratedMessager(Messager messager) {
    this.messager = messager;
  }

  /** Emits a global warning that doesn't target a particular element. */
  public void warn(String template, Object... arguments) {
    messager.printMessage(Diagnostic.Kind.WARNING, String.format(template, arguments));
  }

  /** Emits a warning for a type. */
  public void warn(TypeElement typeElement, String template, Object... arguments) {
    messager.printMessage(Diagnostic.Kind.WARNING, String.format(template, arguments), typeElement);
  }

  /** Emits an error for a type. */
  public void error(TypeElement typeElement, String template, Object... arguments) {
    messager.printMessage(Diagnostic.Kind.ERROR, String.format(template, arguments), typeElement);
  }

  /**
   * Emits a warning for a program element that might be inherited from another type.
   *
   * @param targetElement the element to target.
   * @param processedType the type that we were processing when we detected the issue.
   */
  public void warn(
      Element targetElement, TypeElement processedType, String template, Object... arguments) {
    new ElementMessager(targetElement, processedType)
        .print(Diagnostic.Kind.WARNING, template, arguments);
  }

  /**
   * Emits an error for a program element that might be inherited from another type.
   *
   * @param targetElement the element to target.
   * @param processedType the type that we were processing when we detected the issue.
   */
  public void error(
      Element targetElement, TypeElement processedType, String template, Object... arguments) {
    new ElementMessager(targetElement, processedType)
        .print(Diagnostic.Kind.ERROR, template, arguments);
  }

  /**
   * Abstracts logic to produce better messages if the target element is inherited from a compiled
   * type.
   *
   * <p>Consider the following situation:
   *
   * <pre>
   *   interface BaseDao {
   *     &#64;Select
   *     void select();
   *   }
   *   &#64;Dao
   *   interface ConcreteDao extends BaseDao {}
   * </pre>
   *
   * If {@code BaseDao} belongs to a JAR dependency, it is already compiled and the warning or error
   * message can't reference a file or line number, it doesn't even mention {@code ConcreteDao}.
   *
   * <p>The goal of this class is to detect those cases, and issue the message on {@code
   * ConcreteDao} instead.
   */
  private class ElementMessager {

    private final Element actualTargetElement;
    // Additional location information that will get prepended to the message
    private final String locationInfo;

    /**
     * @param processedType the type that we are currently processing ({@code ConcreteDao} in the
     *     example above).
     */
    ElementMessager(@NonNull Element intendedTargetElement, @NonNull TypeElement processedType) {

      TypeElement declaringType;
      switch (intendedTargetElement.getKind()) {
        case CLASS:
        case INTERFACE:
          if (processedType.equals(intendedTargetElement)
              || isSourceFile((TypeElement) intendedTargetElement)) {
            this.actualTargetElement = intendedTargetElement;
            this.locationInfo = "";
          } else {
            this.actualTargetElement = processedType;
            this.locationInfo =
                String.format("[Ancestor %s]", intendedTargetElement.getSimpleName());
          }
          break;
        case FIELD:
        case METHOD:
        case CONSTRUCTOR:
          declaringType = (TypeElement) intendedTargetElement.getEnclosingElement();
          if (processedType.equals(declaringType) || isSourceFile(declaringType)) {
            this.actualTargetElement = intendedTargetElement;
            this.locationInfo = "";
          } else {
            this.actualTargetElement = processedType;
            this.locationInfo =
                String.format(
                    "[%s inherited from %s] ",
                    intendedTargetElement, declaringType.getSimpleName());
          }
          break;
        case PARAMETER:
          ExecutableElement method =
              (ExecutableElement) intendedTargetElement.getEnclosingElement();
          declaringType = (TypeElement) method.getEnclosingElement();
          if (processedType.equals(declaringType) || isSourceFile(declaringType)) {
            this.actualTargetElement = intendedTargetElement;
            this.locationInfo = "";
          } else {
            this.actualTargetElement = processedType;
            this.locationInfo =
                String.format(
                    "[Parameter %s of %s inherited from %s] ",
                    intendedTargetElement.getSimpleName(),
                    method.getSimpleName(),
                    declaringType.getSimpleName());
          }
          break;
        default:
          // We don't emit messages for other types of elements in the mapper processor. Handle
          // gracefully nevertheless:
          this.actualTargetElement = intendedTargetElement;
          this.locationInfo = "";
          break;
      }
    }

    void print(Diagnostic.Kind level, String template, Object... arguments) {
      messager.printMessage(
          level, String.format(locationInfo + template, arguments), actualTargetElement);
    }

    private boolean isSourceFile(TypeElement element) {
      try {
        Class.forName(element.getQualifiedName().toString());
        return false;
      } catch (ClassNotFoundException e) {
        return true;
      }
    }
  }
}
