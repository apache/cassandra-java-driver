/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.core.utils;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class StringConstructorTest {

  @Test(groups = "unit")
  public void testItWorks() {
    String strInput = "the quick brown fox jumps over the lazy dog";
    char[] input = strInput.toCharArray();
    Bytes.StringConstructor c = Bytes.createStringConstructor();
    final String output = c.construct(input);
    assertEquals(output, strInput);
  }

}
