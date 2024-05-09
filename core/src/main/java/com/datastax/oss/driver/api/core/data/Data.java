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
package com.datastax.oss.driver.api.core.data;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.detach.Detachable;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import edu.umd.cs.findbugs.annotations.NonNull;

/** A data structure containing CQL values. */
public interface Data {

  /**
   * Returns the registry of all the codecs currently available to convert values for this instance.
   *
   * <p>If you obtained this object from the driver, this will be set automatically. If you created
   * it manually, or just deserialized it, it is set to {@link CodecRegistry#DEFAULT}. You can
   * reattach this object to an existing driver instance to use its codec registry.
   *
   * @see Detachable
   */
  @NonNull
  CodecRegistry codecRegistry();

  /**
   * Returns the protocol version that is currently used to convert values for this instance.
   *
   * <p>If you obtained this object from the driver, this will be set automatically. If you created
   * it manually, or just deserialized it, it is set to {@link DefaultProtocolVersion#DEFAULT}. You
   * can reattach this object to an existing driver instance to use its protocol version.
   *
   * @see Detachable
   */
  @NonNull
  ProtocolVersion protocolVersion();
}
