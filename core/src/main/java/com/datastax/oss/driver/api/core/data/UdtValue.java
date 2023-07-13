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

import com.datastax.oss.driver.api.core.detach.Detachable;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Driver-side representation of an instance of a CQL user defined type.
 *
 * <p>It is an ordered set of named, typed fields.
 *
 * <p>A tuple value is attached if and only if its type is attached (see {@link Detachable}).
 *
 * <p>The default implementation returned by the driver is immutable and serializable. If you write
 * your own implementation, it should at least be thread-safe; serializability is not mandatory, but
 * recommended for use with some 3rd-party tools like Apache Spark &trade;.
 */
public interface UdtValue
    extends GettableById, GettableByName, SettableById<UdtValue>, SettableByName<UdtValue> {

  @NonNull
  UserDefinedType getType();
}
