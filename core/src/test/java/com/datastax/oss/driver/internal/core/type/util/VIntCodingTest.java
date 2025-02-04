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
package com.datastax.oss.driver.internal.core.type.util;

import static org.junit.Assert.assertEquals;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.nio.ByteBuffer;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class VIntCodingTest {
  @DataProvider
  public static Object[] roundTripTestValues() {
    return new Integer[] {
      Integer.MAX_VALUE + 1,
      Integer.MAX_VALUE,
      Integer.MAX_VALUE - 1,
      Integer.MIN_VALUE,
      Integer.MIN_VALUE + 1,
      Integer.MIN_VALUE - 1,
      0,
      -1,
      1
    };
  };

  private static final long[] LONGS =
      new long[] {
        53L,
        10201L,
        1097151L,
        168435455L,
        33251130335L,
        3281283447775L,
        417672546086779L,
        52057592037927932L,
        72057594037927937L
      };

  @Test
  public void should_compute_unsigned_vint_size() {
    for (int i = 0; i < LONGS.length; i++) {
      long val = LONGS[i];
      assertEquals(i + 1, VIntCoding.computeUnsignedVIntSize(val));
    }
  }

  @Test
  @UseDataProvider("roundTripTestValues")
  public void should_write_and_read_unsigned_vint_32(int value) {
    ByteBuffer bb = ByteBuffer.allocate(9);

    VIntCoding.writeUnsignedVInt32(value, bb);
    bb.flip();
    assertEquals(value, VIntCoding.getUnsignedVInt32(bb, 0));
  }

  @Test
  @UseDataProvider("roundTripTestValues")
  public void should_write_and_read_unsigned_vint(int value) {
    ByteBuffer bb = ByteBuffer.allocate(9);

    VIntCoding.writeUnsignedVInt(value, bb);
    bb.flip();
    assertEquals(value, VIntCoding.getUnsignedVInt(bb, 0));
  }
}
