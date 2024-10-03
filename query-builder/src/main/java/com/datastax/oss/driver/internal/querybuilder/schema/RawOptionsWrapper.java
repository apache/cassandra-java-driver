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
package com.datastax.oss.driver.internal.querybuilder.schema;

import com.datastax.oss.driver.api.core.data.ByteUtils;

/**
 * Wrapper class to indicate that the contained String value should be understood to represent a CQL
 * literal that can be included directly in a CQL statement (i.e. without escaping).
 */
public class RawOptionsWrapper {
  private final String val;

  private RawOptionsWrapper(String val) {
    this.val = val;
  }

  public static RawOptionsWrapper of(String val) {
    return new RawOptionsWrapper(val);
  }

  public static RawOptionsWrapper of(byte[] val) {
    return new RawOptionsWrapper(ByteUtils.toHexString(val));
  }

  @Override
  public String toString() {
    return this.val;
  }
}
