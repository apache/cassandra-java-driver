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
package com.datastax.oss.driver.internal.core.protocol;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.internal.core.util.ByteBufs;
import com.datastax.oss.protocol.internal.util.Bytes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import org.junit.Test;

/**
 * Note: we don't test trivial methods that simply delegate to ByteBuf, nor default implementations
 * inherited from {@link com.datastax.oss.protocol.internal.PrimitiveCodec}.
 */
public class ByteBufPrimitiveCodecTest {
  private ByteBufPrimitiveCodec codec = new ByteBufPrimitiveCodec(ByteBufAllocator.DEFAULT);

  @Test
  public void should_concatenate() {
    ByteBuf left = ByteBufs.wrap(0xca, 0xfe);
    ByteBuf right = ByteBufs.wrap(0xba, 0xbe);
    assertThat(codec.concat(left, right)).containsExactly("0xcafebabe");
  }

  @Test
  public void should_concatenate_slices() {
    ByteBuf left = ByteBufs.wrap(0x00, 0xca, 0xfe, 0x00).slice(1, 2);
    ByteBuf right = ByteBufs.wrap(0x00, 0x00, 0xba, 0xbe, 0x00).slice(2, 2);

    assertThat(codec.concat(left, right)).containsExactly("0xcafebabe");
  }

  @Test
  public void should_read_inet_v4() {
    ByteBuf source =
        ByteBufs.wrap(
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
    ByteBuf lengthAndAddress = allocate(17);
    lengthAndAddress.writeByte(16);
    lengthAndAddress.writeLong(0);
    lengthAndAddress.writeLong(1);
    ByteBuf source =
        codec.concat(
            lengthAndAddress,
            // port (as an int)
            ByteBufs.wrap(0x00, 0x00, 0x23, 0x52));
    InetSocketAddress inet = codec.readInet(source);
    assertThat(inet.getAddress().getHostAddress()).isEqualTo("0:0:0:0:0:0:0:1");
    assertThat(inet.getPort()).isEqualTo(9042);
  }

  @Test
  public void should_fail_to_read_inet_if_length_invalid() {
    ByteBuf source =
        ByteBufs.wrap(
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
    ByteBuf source =
        ByteBufs.wrap(
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
    ByteBuf source = allocate(17);
    source.writeByte(16);
    source.writeLong(0);
    source.writeLong(1);
    InetAddress inetAddr = codec.readInetAddr(source);
    assertThat(inetAddr.getHostAddress()).isEqualTo("0:0:0:0:0:0:0:1");
  }

  @Test
  public void should_fail_to_read_inetaddr_if_length_invalid() {
    ByteBuf source =
        ByteBufs.wrap(
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
    ByteBuf source =
        ByteBufs.wrap(
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
  public void should_read_bytes_when_extra_data() {
    ByteBuf source =
        ByteBufs.wrap(
            // length (as an int)
            0x00,
            0x00,
            0x00,
            0x04,
            // contents
            0xca,
            0xfe,
            0xba,
            0xbe,
            0xde,
            0xda,
            0xdd);
    ByteBuffer bytes = codec.readBytes(source);
    assertThat(Bytes.toHexString(bytes)).isEqualTo("0xcafebabe");
  }

  @Test
  public void read_bytes_should_udpate_reader_index() {
    ByteBuf source =
        ByteBufs.wrap(
            // length (as an int)
            0x00,
            0x00,
            0x00,
            0x04,
            // contents
            0xca,
            0xfe,
            0xba,
            0xbe,
            0xde,
            0xda,
            0xdd);
    codec.readBytes(source);

    assertThat(source.readerIndex()).isEqualTo(8);
  }

  @Test
  public void read_bytes_should_throw_when_not_enough_content() {
    ByteBuf source =
        ByteBufs.wrap(
            // length (as an int) : 4 bytes
            0x00,
            0x00,
            0x00,
            0x04,
            // contents : only 2 bytes
            0xca,
            0xfe);
    assertThatThrownBy(() -> codec.readBytes(source)).isInstanceOf(IndexOutOfBoundsException.class);
  }

  @Test
  public void should_read_null_bytes() {
    ByteBuf source = ByteBufs.wrap(0xFF, 0xFF, 0xFF, 0xFF); // -1 (as an int)
    assertThat(codec.readBytes(source)).isNull();
  }

  @Test
  public void should_read_short_bytes() {
    ByteBuf source =
        ByteBufs.wrap(
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

  @Test
  public void should_read_string() {
    ByteBuf source =
        ByteBufs.wrap(
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
    ByteBuf source = codec.allocate(2);
    source.writeShort(4);

    assertThatThrownBy(() -> codec.readString(source))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Not enough bytes to read an UTF-8 serialized string of size 4");
  }

  @Test
  public void should_read_long_string() {
    ByteBuf source =
        ByteBufs.wrap(
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
    ByteBuf source = codec.allocate(4);
    source.writeInt(4);

    assertThatThrownBy(() -> codec.readLongString(source))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Not enough bytes to read an UTF-8 serialized string of size 4");
  }

  @Test
  public void should_write_inet_v4() throws Exception {
    ByteBuf dest = allocate(1 + 4 + 4);
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
    ByteBuf dest = allocate(1 + 16 + 4);
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
    ByteBuf dest = allocate(1 + 4);
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
    ByteBuf dest = allocate(1 + 16);
    InetAddress inetAddr = InetAddress.getByName("::1");
    codec.writeInetAddr(inetAddr, dest);
    assertThat(dest)
        .containsExactly(
            "0x10" // size as a byte
                + "00000000000000000000000000000001" // address
            );
  }

  @Test
  public void should_write_string() {
    ByteBuf dest = allocate(7);
    codec.writeString("hello", dest);
    assertThat(dest)
        .containsExactly(
            "0x0005" // size as an unsigned short
                + "68656c6c6f" // UTF-8 contents
            );
  }

  @Test
  public void should_write_long_string() {
    ByteBuf dest = allocate(9);
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
    ByteBuf dest = allocate(8);
    codec.writeBytes(Bytes.fromHexString("0xcafebabe"), dest);
    assertThat(dest)
        .containsExactly(
            "0x00000004"
                + // size as an int
                "cafebabe");
  }

  @Test
  public void should_write_short_bytes() {
    ByteBuf dest = allocate(6);
    codec.writeShortBytes(new byte[] {(byte) 0xca, (byte) 0xfe, (byte) 0xba, (byte) 0xbe}, dest);
    assertThat(dest)
        .containsExactly(
            "0x0004"
                + // size as an unsigned short
                "cafebabe");
  }

  @Test
  public void should_write_null_bytes() {
    ByteBuf dest = allocate(4);
    codec.writeBytes((ByteBuffer) null, dest);
    assertThat(dest).containsExactly("0xFFFFFFFF");
  }

  private static ByteBuf allocate(int length) {
    return ByteBufAllocator.DEFAULT.buffer(length);
  }
}
