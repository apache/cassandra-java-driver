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
package com.datastax.dse.driver.internal.core.type.codec.geometry;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.data.geometry.Geometry;

public abstract class GeometryCodecTest<G extends Geometry, C extends GeometryCodec<G>> {

  private C codec;

  protected GeometryCodecTest(C codec) {
    this.codec = codec;
  }

  public void should_format(G input, String expected) {
    assertThat(codec.format(input)).isEqualTo(expected);
  }

  public void should_parse(String input, G expected) {
    assertThat(codec.parse(input)).isEqualTo(expected);
  }
}
