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
package com.datastax.oss.driver.internal.mapper.processor.util;

import static com.datastax.oss.driver.Assertions.assertThat;

import org.junit.Test;

public class CapitalizerTest {

  @Test
  public void should_decapitalize_regular_strings() {
    assertThat(Capitalizer.decapitalize("foo")).isEqualTo("foo");
    assertThat(Capitalizer.decapitalize("Foo")).isEqualTo("foo");
    assertThat(Capitalizer.decapitalize("FooBar")).isEqualTo("fooBar");
  }

  @Test
  public void should_not_decapitalize_when_second_char_is_uppercase() {
    assertThat(Capitalizer.decapitalize("ID")).isEqualTo("ID");
    assertThat(Capitalizer.decapitalize("XML")).isEqualTo("XML");
    assertThat(Capitalizer.decapitalize("XMLRequest")).isEqualTo("XMLRequest");
  }

  @Test
  public void should_capitalize_regular_strings() {
    assertThat(Capitalizer.capitalize("foo")).isEqualTo("Foo");
    assertThat(Capitalizer.capitalize("fooBar")).isEqualTo("FooBar");
  }

  @Test
  public void should_not_capitalize_when_second_char_is_uppercase() {
    assertThat(Capitalizer.capitalize("cId")).isEqualTo("cId");
  }

  @Test
  public void should_infer_field_name_and_setter_from_getter() {
    // This is the sequence in which the processor uses those methods
    String getterName = "getcId";
    String fieldName = Capitalizer.decapitalize(getterName.substring(3));
    String setterName = "set" + Capitalizer.capitalize(fieldName);
    assertThat(fieldName).isEqualTo("cId");
    assertThat(setterName).isEqualTo("setcId");
  }
}
