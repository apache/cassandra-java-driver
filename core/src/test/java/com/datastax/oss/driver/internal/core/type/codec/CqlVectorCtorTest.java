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
package com.datastax.oss.driver.internal.core.type.codec;

import com.datastax.oss.driver.api.core.data.CqlVector;
import org.junit.Test;
import java.util.ArrayList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * These tests are here simply to demonstrate the disambiguation on the CqlVector constructors,
 * where List of T as well as varags T... are both provided.
 */
public class CqlVectorCtorTest {

  @Test
  public void shouldAllBeEquivalent() {
    CqlVector withVarargsStatic = CqlVector.of(1.0f, 2.5f);

    ArrayList<Float> qualifiedType = new ArrayList<>();
    qualifiedType.add(1.0f);
    qualifiedType.add(2.5f);
    CqlVector withParameterizedList = new CqlVector(qualifiedType);

    ArrayList unqualifiedGenericType = new ArrayList();
    unqualifiedGenericType.add(1.0f);
    unqualifiedGenericType.add(2.5f);
    CqlVector withInferredType = new CqlVector(unqualifiedGenericType);

    assertThat(withVarargsStatic.getValues()).containsExactly(1.0f, 2.5f);
    assertThat(withParameterizedList.getValues()).containsExactly(1.0f, 2.5f);
    assertThat(withInferredType.getValues()).containsExactly(1.0f, 2.5f);

    assertThat(withVarargsStatic).isEqualTo(withParameterizedList);
    assertThat(withVarargsStatic).isEqualTo(withInferredType);
  }
}
