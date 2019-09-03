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
package com.datastax.driver.core.exceptions;

import com.datastax.driver.core.EndPoint;
import java.net.InetSocketAddress;

// The sole purpose of this class is to allow some exception types to preserve a constructor that
// takes an InetSocketAddress (for backward compatibility).
class WrappingEndPoint implements EndPoint {
  private final InetSocketAddress address;

  WrappingEndPoint(InetSocketAddress address) {
    this.address = address;
  }

  @Override
  public InetSocketAddress resolve() {
    return address;
  }
}
