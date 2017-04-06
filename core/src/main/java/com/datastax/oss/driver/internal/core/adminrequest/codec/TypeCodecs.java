/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.core.adminrequest.codec;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** The codecs we need for admin queries. */
public class TypeCodecs {
  public static final TypeCodec<Boolean> BOOLEAN = new BooleanCodec();
  public static final TypeCodec<Integer> INT = new IntCodec();
  public static final TypeCodec<Double> DOUBLE = new DoubleCodec();
  public static final TypeCodec<String> VARCHAR = new VarcharCodec();
  public static final TypeCodec<UUID> UUID = new UuidCodec();
  public static final TypeCodec<ByteBuffer> BLOB = new BlobCodec();
  public static final TypeCodec<InetAddress> INET = new InetCodec();
  public static final TypeCodec<List<String>> LIST_OF_VARCHAR = new ListCodec<>(VARCHAR);
  public static final TypeCodec<Set<String>> SET_OF_VARCHAR = new SetCodec<>(VARCHAR);
  public static final TypeCodec<Map<String, String>> MAP_OF_VARCHAR_TO_VARCHAR =
      new MapCodec<>(VARCHAR, VARCHAR);
  public static final TypeCodec<Map<String, ByteBuffer>> MAP_OF_VARCHAR_TO_BLOB =
      new MapCodec<>(VARCHAR, BLOB);
  public static final TypeCodec<Map<UUID, ByteBuffer>> MAP_OF_UUID_TO_BLOB =
      new MapCodec<>(UUID, BLOB);
}
