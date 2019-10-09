/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
