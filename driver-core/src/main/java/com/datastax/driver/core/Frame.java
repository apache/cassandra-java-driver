/*
 *      Copyright (C) 2012 DataStax Inc.
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

import java.io.IOException;
import java.util.EnumSet;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.CorruptedFrameException;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

import com.datastax.driver.core.exceptions.DriverInternalError;

class Frame {

    public final Header header;
    public final ChannelBuffer body;

    /**
     * On-wire frame.
     * Frames for protocol version 1+2 are defined as:
     *
     *   0         8        16        24        32
     *   +---------+---------+---------+---------+
     *   | version |  flags  | stream  | opcode  |
     *   +---------+---------+---------+---------+
     *   |                length                 |
     *   +---------+---------+---------+---------+
     *
     * Frames for protocol version 3 are defined as:
     *
     *   0         8        16        24        32
     *   +---------+---------+---------+---------+
     *   | version |  flags  |      stream       |
     *   +---------+---------+---------+---------+
     *   | opcode  |      length                 |
     *   +---------+---------+---------+---------+
     *   | length  |
     *   +---------+
     */
    private Frame(Header header, ChannelBuffer body) {
        this.header = header;
        this.body = body;
    }

    static Frame create(ChannelBuffer fullFrame) {
        int version = fullFrame.readByte();
        // version first bit is the "direction" of the frame (request or response)
        version = version & 0x7F;

        int hdrLen = version >= 3 ? Header.LENGTH_V3 : Header.LENGTH_V1;

        assert fullFrame.readableBytes() >= (hdrLen-1) : String.format("Frame too short (%d bytes)", fullFrame.readableBytes());

        int flags = fullFrame.readByte();
        int streamId = hdrLen == 9 ? fullFrame.readShort() : fullFrame.readByte();
        int opcode = fullFrame.readByte();
        int length = fullFrame.readInt();
        assert length == fullFrame.readableBytes();

        Header header = new Header(version, flags, streamId, opcode);
        return new Frame(header, fullFrame);
    }

    public static Frame create(int version, int opcode, int streamId, EnumSet<Header.Flag> flags, ChannelBuffer body) {
        Header header = new Header(version, flags, streamId, opcode);
        return new Frame(header, body);
    }

    public static class Header {

        public static final int LENGTH_V1 = 8;
        public static final int LENGTH_V3 = 9;

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

        public enum Flag
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

    public Frame with(ChannelBuffer newBody) {
        return new Frame(header, newBody);
    }

    public static final class Decoder extends FrameDecoder {
        static final DecoderV1 decoderV1 = new DecoderV1();
        static final DecoderV3 decoderV3 = new DecoderV3();

        @Override protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
            int version = buffer.getByte(0);
            // version first bit is the "direction" of the frame (request or response)
            version = version & 0x7F;

            return version >= 3 ? decoderV3.decode(ctx, channel, buffer) : decoderV1.decode(ctx, channel, buffer);
        }
    }

    public static class DecoderV1 extends LengthFieldBasedFrameDecoder {

        private static final int MAX_FRAME_LENTH = 256 * 1024 * 1024; // 256 MB

        public DecoderV1() {
            super(MAX_FRAME_LENTH, 4, 4, 0, 0, true);
        }

        @Override
        protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
            try {
                if (buffer.readableBytes() < 4)
                    return null;

                // Validate the opcode (this will throw if it's not a response)
                Message.Response.Type.fromOpcode(buffer.getByte(3));

                ChannelBuffer frame = (ChannelBuffer) super.decode(ctx, channel, buffer);
                if (frame == null) {
                    return null;
                }

                return Frame.create(frame);
            } catch (CorruptedFrameException e) {
                throw new DriverInternalError(e.getMessage());
            } catch (TooLongFrameException e) {
                throw new DriverInternalError(e.getMessage());
            }
        }
    }

    public static class DecoderV3 extends LengthFieldBasedFrameDecoder {

        private static final int MAX_FRAME_LENTH = 256 * 1024 * 1024; // 256 MB

        public DecoderV3() {
            super(MAX_FRAME_LENTH, 5, 4, 0, 0, true);
        }

        @Override
        protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
            try {
                if (buffer.readableBytes() < 4)
                    return null;

                // Validate the opcode (this will throw if it's not a response)
                Message.Response.Type.fromOpcode(buffer.getByte(4));

                ChannelBuffer frame = (ChannelBuffer) super.decode(ctx, channel, buffer);
                if (frame == null) {
                    return null;
                }

                return Frame.create(frame);
            } catch (CorruptedFrameException e) {
                throw new DriverInternalError(e.getMessage());
            } catch (TooLongFrameException e) {
                throw new DriverInternalError(e.getMessage());
            }
        }
    }

    public static class Encoder extends OneToOneEncoder {

        @Override public Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws IOException {
            assert msg instanceof Frame : "Expecting frame, got " + msg;

            Frame frame = (Frame)msg;

            ChannelBuffer header = ChannelBuffers.buffer(frame.header.version >= 3 ? Header.LENGTH_V3 : Header.LENGTH_V1);
            // We don't bother with the direction, we only send requests.
            header.writeByte(frame.header.version);
            header.writeByte(Header.Flag.serialize(frame.header.flags));
            if (frame.header.version >= 3)
                header.writeShort(frame.header.streamId);
            else
                header.writeByte(frame.header.streamId);
            header.writeByte(frame.header.opcode);
            header.writeInt(frame.body.readableBytes());
            return ChannelBuffers.wrappedBuffer(header, frame.body);
        }
    }

    public static class Decompressor extends OneToOneDecoder {

        private final FrameCompressor compressor;

        public Decompressor(FrameCompressor compressor) {
            assert compressor != null;
            this.compressor = compressor;
        }

        @Override public Object decode(ChannelHandlerContext ctx, Channel channel, Object msg) throws IOException {

            assert msg instanceof Frame : "Expecting frame, got " + msg;

            Frame frame = (Frame)msg;

            return frame.header.flags.contains(Header.Flag.COMPRESSED)
                 ? compressor.decompress(frame)
                 : frame;
        }
    }

    public static class Compressor extends OneToOneEncoder {

        private final FrameCompressor compressor;

        public Compressor(FrameCompressor compressor) {
            assert compressor != null;
            this.compressor = compressor;
        }

        @Override public Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws IOException {

            assert msg instanceof Frame : "Expecting frame, got " + msg;

            Frame frame = (Frame)msg;

            // Never compress STARTUP messages
            if (frame.header.opcode == Message.Request.Type.STARTUP.opcode)
                return frame;

            frame.header.flags.add(Header.Flag.COMPRESSED);
            return compressor.compress(frame);
        }
    }
}
