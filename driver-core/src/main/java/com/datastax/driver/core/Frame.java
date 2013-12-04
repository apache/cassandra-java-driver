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
     * Frames are defined as:
     *
     *   0         8        16        24        32
     *   +---------+---------+---------+---------+
     *   | version |  flags  | stream  | opcode  |
     *   +---------+---------+---------+---------+
     *   |                length                 |
     *   +---------+---------+---------+---------+
     */
    private Frame(Header header, ChannelBuffer body) {
        this.header = header;
        this.body = body;
    }

    public static Frame create(ChannelBuffer fullFrame) {

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

    public static Frame create(int opcode, int streamId, EnumSet<Header.Flag> flags, ChannelBuffer body) {
        Header header = new Header(Header.CURRENT_VERSION, flags, streamId, opcode);
        return new Frame(header, body);
    }

    public static class Header {

        public static final int CURRENT_VERSION = 2;
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

    public Frame with(ChannelBuffer newBody) {
        return new Frame(header, newBody);
    }

    public static class Decoder extends LengthFieldBasedFrameDecoder {

        private static final int MAX_FRAME_LENTH = 256 * 1024 * 1024; // 256 MB

        public Decoder() {
            super(MAX_FRAME_LENTH, 4, 4, 0, 0, true);
        }

        @Override
        protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
            try {
                // We must at least validate that the frame version is something we support/know and it doesn't hurt to
                // check the opcode is not garbage. And we should do that indenpently of what is the the bytes corresponding
                // to the frame length are, i.e. we shouldn't wait for super.decode() to return non-null.
                if (buffer.readableBytes() == 0)
                    return null;

                int firstByte = buffer.getByte(0);
                int version = firstByte & 0x7F;

                if (version != Header.CURRENT_VERSION)
                    throw new DriverInternalError("Server response from unsupported protocol version: " + version);

                // Validate the opcode (this will throw if it's not a response)
                if (buffer.readableBytes() >= 4)
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

    public static class Encoder extends OneToOneEncoder {

        // We'll only send requests...
        private static int VERSION_AND_DIRECTION = 0x02;

        public Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws IOException {
            assert msg instanceof Frame : "Expecting frame, got " + msg;

            Frame frame = (Frame)msg;

            ChannelBuffer header = ChannelBuffers.buffer(Frame.Header.LENGTH);
            header.writeByte(VERSION_AND_DIRECTION);
            header.writeByte(Header.Flag.serialize(frame.header.flags));
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

        public Object decode(ChannelHandlerContext ctx, Channel channel, Object msg) throws IOException {

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

        public Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws IOException {

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
