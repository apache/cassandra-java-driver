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
package com.datastax.oss.driver.internal.core.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets.SetView;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;

public class CompressSubstitutionsTest {

  @Test
  public void Lz4SubstitutionShouldSubstituteAllProtectedMethodsFromLz4Compressor() {
    // given
    List<Method> compressorMethods = getProtectedMethods(Lz4Compressor.class);
    List<Method> substitutionMethods = getProtectedMethods(Lz4Substitution.class);

    // when
    Set<MethodNameWithoutClass> compressorMethodsWithoutClassName =
        compressorMethods.stream()
            .map(this::toMethodNameIgnoringDeclaringClass)
            .collect(Collectors.toSet());
    Set<MethodNameWithoutClass> substitutionMethodsWithoutClassName =
        substitutionMethods.stream()
            .map(this::toMethodNameIgnoringDeclaringClass)
            .collect(Collectors.toSet());

    // then
    SetView<MethodNameWithoutClass> difference =
        Sets.difference(compressorMethodsWithoutClassName, substitutionMethodsWithoutClassName);
    assertThat(difference).isEmpty();
  }

  @Test
  public void SnappySubstitutionShouldSubstituteAllProtectedMethodsFromSnappyCompressor() {
    // given
    List<Method> compressorMethods = getProtectedMethods(SnappyCompressor.class);
    List<Method> substitutionMethods = getProtectedMethods(SnappySubstitution.class);

    // when
    Set<MethodNameWithoutClass> compressorMethodsWithoutClassName =
        compressorMethods.stream()
            .map(this::toMethodNameIgnoringDeclaringClass)
            .collect(Collectors.toSet());
    Set<MethodNameWithoutClass> substitutionMethodsWithoutClassName =
        substitutionMethods.stream()
            .map(this::toMethodNameIgnoringDeclaringClass)
            .collect(Collectors.toSet());

    // then
    SetView<MethodNameWithoutClass> difference =
        Sets.difference(compressorMethodsWithoutClassName, substitutionMethodsWithoutClassName);
    assertThat(difference).isEmpty();
  }

  public static List<Method> getProtectedMethods(Class<?> clazz) {
    List<Method> result = new ArrayList<>();

    for (Method method : clazz.getDeclaredMethods()) {
      int modifiers = method.getModifiers();
      if (Modifier.isProtected(modifiers)) {
        result.add(method);
      }
    }

    return result;
  }

  private MethodNameWithoutClass toMethodNameIgnoringDeclaringClass(Method method) {
    return new MethodNameWithoutClass(
        method.getName(), method.getReturnType(), method.getParameters());
  }

  private static class MethodNameWithoutClass {
    private final String name;

    private final Class<?> returnType;

    private final List<Class<?>> parameters;

    public MethodNameWithoutClass(String name, Class<?> returnType, Parameter[] parameters) {

      this.name = name;
      this.returnType = returnType;
      this.parameters =
          Arrays.stream(parameters).map(Parameter::getType).collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof MethodNameWithoutClass)) return false;

      MethodNameWithoutClass that = (MethodNameWithoutClass) o;

      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(returnType, that.returnType)) return false;
      return Objects.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, returnType, parameters);
    }

    @Override
    public String toString() {
      return "MethodNameWithoutClass{"
          + "name='"
          + name
          + '\''
          + ", returnType="
          + returnType
          + ", parameters="
          + parameters
          + '}';
    }
  }
}
