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
package com.datastax.dse.driver.internal.core.protocol;

import static com.datastax.dse.driver.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.dse.driver.Assertions;
import com.datastax.dse.driver.internal.core.graph.binary.buffer.DseNettyBufferFactory;
import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodecTest;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.function.Supplier;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Note: like {@link ByteBufPrimitiveCodecTest} we don't test trivial methods that simply delegate
 * to the underlying Buffer, nor default implementations inherited from {@link
 * com.datastax.oss.protocol.internal.PrimitiveCodec}.
 */
@RunWith(DataProviderRunner.class)
public class TinkerpopBufferPrimitiveCodecTest {

  private static final DseNettyBufferFactory factory = new DseNettyBufferFactory();
  private final TinkerpopBufferPrimitiveCodec codec = new TinkerpopBufferPrimitiveCodec(factory);

  @Test
  public void should_concatenate() {
    Buffer left = factory.withBytes(0xca, 0xfe);
    Buffer right = factory.withBytes(0xba, 0xbe);
    assertThat(codec.concat(left, right)).containsExactly("0xcafebabe");
  }

  @Test
  public void should_read_inet_v4() {
    Buffer source =
        factory.withBytes(
            // length (as a byte)
            0x04,
            // address
            0x7f,
            0x00,
            0x00,
            0x01,
            // port (as an int)
            0x00,
            0x00,
            0x23,
            0x52);
    InetSocketAddress inet = codec.readInet(source);
    assertThat(inet.getAddress().getHostAddress()).isEqualTo("127.0.0.1");
    assertThat(inet.getPort()).isEqualTo(9042);
  }

  @Test
  public void should_read_inet_v6() {
    Buffer lengthAndAddress = factory.heap(17);
    lengthAndAddress.writeByte(16);
    lengthAndAddress.writeLong(0);
    lengthAndAddress.writeLong(1);
    Buffer source =
        codec.concat(
            lengthAndAddress,
            // port (as an int)
            factory.withBytes(0x00, 0x00, 0x23, 0x52));
    InetSocketAddress inet = codec.readInet(source);
    assertThat(inet.getAddress().getHostAddress()).isEqualTo("0:0:0:0:0:0:0:1");
    assertThat(inet.getPort()).isEqualTo(9042);
  }

  @Test
  public void should_fail_to_read_inet_if_length_invalid() {
    Buffer source =
        factory.withBytes(
            // length (as a byte)
            0x03,
            // address
            0x7f,
            0x00,
            0x01,
            // port (as an int)
            0x00,
            0x00,
            0x23,
            0x52);
    assertThatThrownBy(() -> codec.readInet(source))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid address length: 3 ([127, 0, 1])");
  }

  @Test
  public void should_read_inetaddr_v4() {
    Buffer source =
        factory.withBytes(
            // length (as a byte)
            0x04,
            // address
            0x7f,
            0x00,
            0x00,
            0x01);
    InetAddress inetAddr = codec.readInetAddr(source);
    assertThat(inetAddr.getHostAddress()).isEqualTo("127.0.0.1");
  }

  @Test
  public void should_read_inetaddr_v6() {
    Buffer source = factory.heap(17);
    source.writeByte(16);
    source.writeLong(0);
    source.writeLong(1);
    InetAddress inetAddr = codec.readInetAddr(source);
    assertThat(inetAddr.getHostAddress()).isEqualTo("0:0:0:0:0:0:0:1");
  }

  @Test
  public void should_fail_to_read_inetaddr_if_length_invalid() {
    Buffer source =
        factory.withBytes(
            // length (as a byte)
            0x03,
            // address
            0x7f,
            0x00,
            0x01);
    assertThatThrownBy(() -> codec.readInetAddr(source))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid address length: 3 ([127, 0, 1])");
  }

  @Test
  public void should_read_bytes() {
    Buffer source =
        factory.withBytes(
            // length (as an int)
            0x00,
            0x00,
            0x00,
            0x04,
            // contents
            0xca,
            0xfe,
            0xba,
            0xbe);
    ByteBuffer bytes = codec.readBytes(source);
    assertThat(Bytes.toHexString(bytes)).isEqualTo("0xcafebabe");
  }

  @Test
  public void should_read_null_bytes() {
    Buffer source = factory.withBytes(0xFF, 0xFF, 0xFF, 0xFF); // -1 (as an int)
    assertThat(codec.readBytes(source)).isNull();
  }

  @Test
  public void should_read_short_bytes() {
    Buffer source =
        factory.withBytes(
            // length (as an unsigned short)
            0x00,
            0x04,
            // contents
            0xca,
            0xfe,
            0xba,
            0xbe);
    assertThat(Bytes.toHexString(codec.readShortBytes(source))).isEqualTo("0xcafebabe");
  }

  @DataProvider
  public static Object[][] bufferTypes() {
    return new Object[][] {
      {(Supplier<Buffer>) factory::heap},
      {(Supplier<Buffer>) factory::io},
      {(Supplier<Buffer>) factory::direct}
    };
  }

  @Test
  @UseDataProvider("bufferTypes")
  public void should_read_string(Supplier<Buffer> supplier) {
    Buffer source =
        factory.withBytes(
            supplier,
            // length (as an unsigned short)
            0x00,
            0x05,
            // UTF-8 contents
            0x68,
            0x65,
            0x6c,
            0x6c,
            0x6f);
    assertThat(codec.readString(source)).isEqualTo("hello");
  }

  @Test
  public void should_fail_to_read_string_if_not_enough_characters() {
    Buffer source = factory.heap();
    source.writeShort(4);

    assertThatThrownBy(() -> codec.readString(source))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Not enough bytes to read an UTF-8 serialized string of size 4");
  }

  @Test
  public void should_read_long_string() {
    Buffer source =
        factory.withBytes(
            // length (as an int)
            0x00,
            0x00,
            0x00,
            0x05,
            // UTF-8 contents
            0x68,
            0x65,
            0x6c,
            0x6c,
            0x6f);
    assertThat(codec.readLongString(source)).isEqualTo("hello");
  }

  @Test
  public void should_fail_to_read_long_string_if_not_enough_characters() {
    Buffer source = factory.heap(4, 4);
    source.writeInt(4);

    assertThatThrownBy(() -> codec.readLongString(source))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Not enough bytes to read an UTF-8 serialized string of size 4");
  }

  @Test
  public void should_write_inet_v4() throws Exception {
    Buffer dest = factory.heap(1 + 4 + 4);
    InetSocketAddress inet = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9042);
    codec.writeInet(inet, dest);
    assertThat(dest)
        .containsExactly(
            "0x04" // size as a byte
                + "7f000001" // address
                + "00002352" // port
            );
  }

  @Test
  public void should_write_inet_v6() throws Exception {
    Buffer dest = factory.heap(1 + 16 + 4);
    InetSocketAddress inet = new InetSocketAddress(InetAddress.getByName("::1"), 9042);
    codec.writeInet(inet, dest);
    assertThat(dest)
        .containsExactly(
            "0x10" // size as a byte
                + "00000000000000000000000000000001" // address
                + "00002352" // port
            );
  }

  @Test
  public void should_write_inetaddr_v4() throws Exception {
    Buffer dest = factory.heap(1 + 4);
    InetAddress inetAddr = InetAddress.getByName("127.0.0.1");
    codec.writeInetAddr(inetAddr, dest);
    assertThat(dest)
        .containsExactly(
            "0x04" // size as a byte
                + "7f000001" // address
            );
  }

  @Test
  public void should_write_inetaddr_v6() throws Exception {
    Buffer dest = factory.heap(1 + 16);
    InetAddress inetAddr = InetAddress.getByName("::1");
    codec.writeInetAddr(inetAddr, dest);
    Assertions.assertThat(dest)
        .containsExactly(
            "0x10" // size as a byte
                + "00000000000000000000000000000001" // address
            );
  }

  @Test
  public void should_write_string() {
    Buffer dest = factory.heap();
    codec.writeString("hello", dest);
    assertThat(dest)
        .containsExactly(
            "0x0005" // size as an unsigned short
                + "68656c6c6f" // UTF-8 contents
            );
  }

  @Test
  public void should_write_long_string() {
    Buffer dest = factory.heap(9);
    codec.writeLongString("hello", dest);
    assertThat(dest)
        .containsExactly(
            "0x00000005"
                + // size as an int
                "68656c6c6f" // UTF-8 contents
            );
  }

  @Test
  public void should_write_bytes() {
    Buffer dest = factory.heap(8);
    codec.writeBytes(Bytes.fromHexString("0xcafebabe"), dest);
    assertThat(dest)
        .containsExactly(
            "0x00000004"
                + // size as an int
                "cafebabe");
  }

  @Test
  public void should_write_short_bytes() {
    Buffer dest = factory.heap(6);
    codec.writeShortBytes(new byte[] {(byte) 0xca, (byte) 0xfe, (byte) 0xba, (byte) 0xbe}, dest);
    assertThat(dest)
        .containsExactly(
            "0x0004"
                + // size as an unsigned short
                "cafebabe");
  }

  @Test
  public void should_write_null_bytes() {
    Buffer dest = factory.heap(4);
    codec.writeBytes((ByteBuffer) null, dest);
    assertThat(dest).containsExactly("0xFFFFFFFF");
  }
}
