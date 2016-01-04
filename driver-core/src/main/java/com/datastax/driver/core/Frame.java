/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.DriverInternalError;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.*;

import java.util.EnumSet;
import java.util.List;

/**
 * A frame for the CQL binary protocol.
 * <p/>
 * Each frame contains a fixed size header (8 bytes for V1 and V2, 9 bytes for V3 and V4)
 * followed by a variable size body. The content of the body depends
 * on the header opcode value (the body can in particular be empty for some
 * opcode values).
 * <p/>
 * The protocol distinguishes 2 types of frames: requests and responses. Requests
 * are those frames sent by the clients to the server, response are the ones sent
 * by the server. Note however that the protocol supports server pushes (events)
 * so responses does not necessarily come right after a client request.
 * <p/>
 * Frames for protocol versions 1+2 are defined as:
 * <p/>
 * <pre>
 *  0         8        16        24        32
 * +---------+---------+---------+---------+
 * | version |  flags  | stream  | opcode  |
 * +---------+---------+---------+---------+
 * |                length                 |
 * +---------+---------+---------+---------+
 * |                                       |
 * .            ...  body ...              .
 * .                                       .
 * .                                       .
 * +---------------------------------------- *
 * </pre>
 * <p/>
 * Frames for protocol versions 3+4 are defined as:
 * <p/>
 * <pre>
 * 0         8        16        24        32         40
 * +---------+---------+---------+---------+---------+
 * | version |  flags  |      stream       | opcode  |
 * +---------+---------+---------+---------+---------+
 * |                length                 |
 * +---------+---------+---------+---------+
 * |                                       |
 * .            ...  body ...              .
 * .                                       .
 * .                                       .
 * +----------------------------------------
 * </pre>
 *
 * @see "https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v1.spec"
 * @see "https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v2.spec"
 * @see "https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v3.spec"
 * @see "https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec"
 */
class Frame {

    final Header header;
    final ByteBuf body;

    private Frame(Header header, ByteBuf body) {
        this.header = header;
        this.body = body;
    }

    private static Frame create(ByteBuf fullFrame) {
        assert fullFrame.readableBytes() >= 1 : String.format("Frame too short (%d bytes)", fullFrame.readableBytes());

        int versionBytes = fullFrame.readByte();
        // version first byte is the "direction" of the frame (request or response)
        ProtocolVersion version = ProtocolVersion.fromInt(versionBytes & 0x7F);
        int hdrLen = Header.lengthFor(version);
        assert fullFrame.readableBytes() >= (hdrLen - 1) : String.format("Frame too short (%d bytes)", fullFrame.readableBytes());

        int flags = fullFrame.readByte();
        int streamId = readStreamid(fullFrame, version);
        int opcode = fullFrame.readByte();
        int length = fullFrame.readInt();
        assert length == fullFrame.readableBytes();

        Header header = new Header(version, flags, streamId, opcode);
        return new Frame(header, fullFrame);
    }

    private static int readStreamid(ByteBuf fullFrame, ProtocolVersion version) {
        switch (version) {
            case V1:
            case V2:
                return fullFrame.readByte();
            case V3:
            case V4:
                return fullFrame.readShort();
            default:
                throw version.unsupported();
        }
    }

    static Frame create(ProtocolVersion version, int opcode, int streamId, EnumSet<Header.Flag> flags, ByteBuf body) {
        Header header = new Header(version, flags, streamId, opcode);
        return new Frame(header, body);
    }

    static class Header {

        final ProtocolVersion version;
        final EnumSet<Flag> flags;
        final int streamId;
        final int opcode;

        private Header(ProtocolVersion version, int flags, int streamId, int opcode) {
            this(version, Flag.deserialize(flags), streamId, opcode);
        }

        private Header(ProtocolVersion version, EnumSet<Flag> flags, int streamId, int opcode) {
            this.version = version;
            this.flags = flags;
            this.streamId = streamId;
            this.opcode = opcode;
        }

        /**
         * Return the expected frame header length in bytes according to the protocol version in use.
         *
         * @param version the protocol version in use
         * @return the expected frame header length in bytes
         */
        static int lengthFor(ProtocolVersion version) {
            switch (version) {
                case V1:
                case V2:
                    return 8;
                case V3:
                case V4:
                    return 9;
                default:
                    throw version.unsupported();
            }
        }

        enum Flag {
            // The order of that enum matters!!
            COMPRESSED,
            TRACING,
            CUSTOM_PAYLOAD,
            WARNING;

            static EnumSet<Flag> deserialize(int flags) {
                EnumSet<Flag> set = EnumSet.noneOf(Flag.class);
                Flag[] values = Flag.values();
                for (int n = 0; n < 8; n++) {
                    if ((flags & (1 << n)) != 0)
                        set.add(values[n]);
                }
                return set;
            }

            static int serialize(EnumSet<Flag> flags) {
                int i = 0;
                for (Flag flag : flags)
                    i |= 1 << flag.ordinal();
                return i;
            }
        }
    }

    Frame with(ByteBuf newBody) {
        return new Frame(header, newBody);
    }

    static final class Decoder extends ByteToMessageDecoder {
        static final DecoderForStreamIdSize decoderV1 = new DecoderForStreamIdSize(1);
        static final DecoderForStreamIdSize decoderV3 = new DecoderForStreamIdSize(2);

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out) throws Exception {
            if (buffer.readableBytes() < 1)
                return;

            int version = buffer.getByte(0);
            // version first bit is the "direction" of the frame (request or response)
            version = version & 0x7F;

            DecoderForStreamIdSize decoder = (version >= 3) ? decoderV3 : decoderV1;
            Object frame = decoder.decode(ctx, buffer);
            if (frame != null)
                out.add(frame);
        }

        static class DecoderForStreamIdSize extends LengthFieldBasedFrameDecoder {
            private static final int MAX_FRAME_LENGTH = 256 * 1024 * 1024; // 256 MB
            private final int opcodeOffset;

            DecoderForStreamIdSize(int streamIdSize) {
                super(MAX_FRAME_LENGTH, /*lengthOffset=*/ 3 + streamIdSize, 4, 0, 0, true);
                this.opcodeOffset = 2 + streamIdSize;
            }

            @Override
            protected Object decode(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
                try {
                    if (buffer.readableBytes() < opcodeOffset + 1)
                        return null;

                    // Validate the opcode (this will throw if it's not a response)
                    Message.Response.Type.fromOpcode(buffer.getByte(opcodeOffset));

                    ByteBuf frame = (ByteBuf) super.decode(ctx, buffer);
                    if (frame == null) {
                        return null;
                    }
                    // Do not deallocate `frame` just yet, because it is stored as Frame.body and will be used
                    // in Message.ProtocolDecoder or Frame.Decompressor if compression is enabled (we deallocate
                    // it there).
                    return Frame.create(frame);
                } catch (CorruptedFrameException e) {
                    throw new DriverInternalError(e.getMessage());
                } catch (TooLongFrameException e) {
                    throw new DriverInternalError(e.getMessage());
                }
            }
        }
    }

    @ChannelHandler.Sharable
    static class Encoder extends MessageToMessageEncoder<Frame> {

        @Override
        protected void encode(ChannelHandlerContext ctx, Frame frame, List<Object> out) throws Exception {
            ProtocolVersion protocolVersion = frame.header.version;
            ByteBuf header = ctx.alloc().ioBuffer(Frame.Header.lengthFor(protocolVersion));
            // We don't bother with the direction, we only send requests.
            header.writeByte(frame.header.version.toInt());
            header.writeByte(Header.Flag.serialize(frame.header.flags));
            writeStreamId(frame.header.streamId, header, protocolVersion);
            header.writeByte(frame.header.opcode);
            header.writeInt(frame.body.readableBytes());

            out.add(header);
            out.add(frame.body);
        }

        private void writeStreamId(int streamId, ByteBuf header, ProtocolVersion protocolVersion) {
            switch (protocolVersion) {
                case V1:
                case V2:
                    header.writeByte(streamId);
                    break;
                case V3:
                case V4:
                    header.writeShort(streamId);
                    break;
                default:
                    throw protocolVersion.unsupported();
            }
        }
    }

    static class Decompressor extends MessageToMessageDecoder<Frame> {

        private final FrameCompressor compressor;

        Decompressor(FrameCompressor compressor) {
            assert compressor != null;
            this.compressor = compressor;
        }

        @Override
        protected void decode(ChannelHandlerContext ctx, Frame frame, List<Object> out) throws Exception {
            if (frame.header.flags.contains(Header.Flag.COMPRESSED)) {
                // All decompressors allocate a new buffer for the decompressed data, so this is the last time
                // we have a reference to the compressed body (and therefore a chance to release it).
                ByteBuf compressedBody = frame.body;
                try {
                    out.add(compressor.decompress(frame));
                } finally {
                    compressedBody.release();
                }
            } else {
                out.add(frame);
            }
        }
    }

    static class Compressor extends MessageToMessageEncoder<Frame> {

        private final FrameCompressor compressor;

        Compressor(FrameCompressor compressor) {
            assert compressor != null;
            this.compressor = compressor;
        }

        @Override
        protected void encode(ChannelHandlerContext ctx, Frame frame, List<Object> out) throws Exception {
            // Never compress STARTUP messages
            if (frame.header.opcode == Message.Request.Type.STARTUP.opcode) {
                out.add(frame);
            } else {
                frame.header.flags.add(Header.Flag.COMPRESSED);
                // See comment in decode()
                ByteBuf uncompressedBody = frame.body;
                try {
                    out.add(compressor.compress(frame));
                } finally {
                    uncompressedBody.release();
                }
            }
        }
    }
}
