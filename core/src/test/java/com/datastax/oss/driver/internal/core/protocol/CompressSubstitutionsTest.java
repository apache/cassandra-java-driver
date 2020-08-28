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

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class CompressSubstitutionsTest {

  private static final Set<String> EXCLUDED_METHOD_NAMES =
      ImmutableSet.of("algorithm", "readUncompressedLength");

  private static final Comparator<Method> METHOD_SIGNATURE_COMPARATOR =
      Comparator.comparing(Method::getName)
          .thenComparing(Method::getReturnType, (ret1, ret2) -> ret1.equals(ret2) ? 0 : -1)
          .thenComparing(
              Method::getParameterTypes,
              (params1, params2) -> Arrays.deepEquals(params1, params2) ? 0 : -1);

  @Test
  @UseDataProvider(value = "substitutionClasses")
  public void should_substitute_compressor_methods(
      Class<?> substitutedClass, Class<?> substitutionClass) {

    Set<Method> methodsToSubstitute =
        Arrays.stream(substitutedClass.getDeclaredMethods())
            .filter(method -> !EXCLUDED_METHOD_NAMES.contains(method.getName()))
            .collect(Collectors.toSet());

    Set<Method> substitutedMethods =
        Arrays.stream(substitutionClass.getDeclaredMethods()).collect(Collectors.toSet());

    assertThat(methodsToSubstitute)
        .usingElementComparator(METHOD_SIGNATURE_COMPARATOR)
        .containsExactlyInAnyOrderElementsOf(substitutedMethods);
  }

  @DataProvider
  public static Object[][] substitutionClasses() {
    return new Object[][] {
      {Lz4Compressor.class, Lz4Substitution.class},
      {SnappyCompressor.class, SnappySubstitution.class}
    };
  }
}
