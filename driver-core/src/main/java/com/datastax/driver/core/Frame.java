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

import java.util.EnumSet;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.*;

import com.datastax.driver.core.exceptions.DriverInternalError;

class Frame {

    public final Header header;
    public final ByteBuf body;

    /**
     * On-wire frame.
     * Frames are defined as:
     *
     *   0         8        16        24        32
     *   +---------+---------+---------+---------+
     *   | version |  flags  | stream  | opcode  |
     *   +---------+---------+---------+---------+
     *   |                length                 |
     *   +---------+---------+---------+---------+
     */
    private Frame(Header header, ByteBuf body) {
        this.header = header;
        this.body = body;
    }

    private static Frame create(ByteBuf fullFrame) {
        assert fullFrame.readableBytes() >= Header.LENGTH : String.format("Frame too short (%d bytes)", fullFrame.readableBytes());

        int version = fullFrame.readByte();
        int flags = fullFrame.readByte();
        int streamId = fullFrame.readByte();
        int opcode = fullFrame.readByte();
        int length = fullFrame.readInt();
        assert length == fullFrame.readableBytes();

        // version first byte is the "direction" of the frame (request or response)
        version = version & 0x7F;

        Header header = new Header(version, flags, streamId, opcode);
        return new Frame(header, fullFrame);
    }

    public static Frame create(int version, int opcode, int streamId, EnumSet<Header.Flag> flags, ByteBuf body) {
        Header header = new Header(version, flags, streamId, opcode);
        return new Frame(header, body);
    }

    public static class Header {

        public static final int LENGTH = 8;

        public final int version;
        public final EnumSet<Flag> flags;
        public final int streamId;
        public final int opcode;

        private Header(int version, int flags, int streamId, int opcode) {
            this(version, Flag.deserialize(flags), streamId, opcode);
        }

        private Header(int version, EnumSet<Flag> flags, int streamId, int opcode) {
            this.version = version;
            this.flags = flags;
            this.streamId = streamId;
            this.opcode = opcode;
        }

        public static enum Flag
        {
            // The order of that enum matters!!
            COMPRESSED,
            TRACING;

            public static EnumSet<Flag> deserialize(int flags) {
                EnumSet<Flag> set = EnumSet.noneOf(Flag.class);
                Flag[] values = Flag.values();
                for (int n = 0; n < 8; n++) {
                    if ((flags & (1 << n)) != 0)
                        set.add(values[n]);
                }
                return set;
            }

            public static int serialize(EnumSet<Flag> flags) {
                int i = 0;
                for (Flag flag : flags)
                    i |= 1 << flag.ordinal();
                return i;
            }
        }
    }

    public Frame with(ByteBuf newBody) {
        return new Frame(header, newBody);
    }

    public static class Decoder extends LengthFieldBasedFrameDecoder {

        private static final int MAX_FRAME_LENTH = 256 * 1024 * 1024; // 256 MB

        public Decoder() {
            super(MAX_FRAME_LENTH, 4, 4, 0, 0, true);
        }

        @Override
        protected Object decode(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
            try {
                if (buffer.readableBytes() == 0)
                    return null;

                // Validate the opcode (this will throw if it's not a response)
                if (buffer.readableBytes() >= 4)
                    Message.Response.Type.fromOpcode(buffer.getByte(3));

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

    @ChannelHandler.Sharable
    public static class Encoder extends MessageToMessageEncoder<Frame> {

        @Override
        protected void encode(ChannelHandlerContext ctx, Frame frame, List<Object> out) throws Exception {
            ByteBuf header = ctx.alloc().ioBuffer(Frame.Header.LENGTH);
            // We don't bother with the direction, we only send requests.
            header.writeByte(frame.header.version);
            header.writeByte(Header.Flag.serialize(frame.header.flags));
            header.writeByte(frame.header.streamId);
            header.writeByte(frame.header.opcode);
            header.writeInt(frame.body.readableBytes());

            out.add(header);
            out.add(frame.body);
        }
    }

    public static class Decompressor extends MessageToMessageDecoder<Frame> {

        private final FrameCompressor compressor;

        public Decompressor(FrameCompressor compressor) {
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

    public static class Compressor extends MessageToMessageEncoder<Frame> {

        private final FrameCompressor compressor;

        public Compressor(FrameCompressor compressor) {
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
