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
package com.datastax.driver.core;

import com.google.common.base.Objects;
import java.net.InetSocketAddress;

/**
 * An endpoint based on server-reported RPC addresses, that might require translation if they are
 * accessed through a proxy.
 */
class TranslatedAddressEndPoint implements EndPoint {

  private final InetSocketAddress translatedAddress;

  TranslatedAddressEndPoint(InetSocketAddress translatedAddress) {
    this.translatedAddress = translatedAddress;
  }

  @Override
  public InetSocketAddress resolve() {
    return translatedAddress;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof TranslatedAddressEndPoint) {
      TranslatedAddressEndPoint that = (TranslatedAddressEndPoint) other;
      return Objects.equal(this.translatedAddress, that.translatedAddress);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return translatedAddress.hashCode();
  }

  @Override
  public String toString() {
    return translatedAddress.toString();
  }
}
