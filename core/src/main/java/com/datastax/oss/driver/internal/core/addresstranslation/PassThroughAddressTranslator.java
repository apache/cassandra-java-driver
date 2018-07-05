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
package com.datastax.oss.driver.internal.core.addresstranslation;

import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.context.DriverContext;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.InetSocketAddress;
import net.jcip.annotations.ThreadSafe;

/**
 * An address translator that always returns the same address unchanged.
 *
 * <p>To activate this translator, modify the {@code advanced.address-translator} section in the
 * driver configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   advanced.address-translator {
 *     class = PassThroughAddressTranslator
 *   }
 * }
 * </pre>
 *
 * See {@code reference.conf} (in the manual or core driver JAR) for more details.
 */
@ThreadSafe
public class PassThroughAddressTranslator implements AddressTranslator {

  public PassThroughAddressTranslator(@SuppressWarnings("unused") DriverContext context) {
    // nothing to do
  }

  @NonNull
  @Override
  public InetSocketAddress translate(@NonNull InetSocketAddress address) {
    return address;
  }

  @Override
  public void close() {
    // nothing to do
  }
}
