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

import java.util.EnumSet;
import java.util.UUID;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.DriverInternalError;

/**
 * A message from the CQL binary protocol.
 */
abstract class Message {

    protected static final Logger logger = LoggerFactory.getLogger(Message.class);

    public interface Coder<R extends Request> {
        public void encode(R request, ChannelBuffer dest);
        public int encodedSize(R request);
    }

    public interface Decoder<R extends Response> {
        public R decode(ChannelBuffer body);
    }

    private volatile int streamId;

    protected Message() {}

    public Message setStreamId(int streamId) {
        this.streamId = streamId;
        return this;
    }

    public int getStreamId() {
        return streamId;
    }

    public static abstract class Request extends Message {

        public enum Type {
            STARTUP        (1,  Requests.Startup.coder),
            OPTIONS        (5,  Requests.Options.coder),
            QUERY          (7,  Requests.Query.coder),
            PREPARE        (9,  Requests.Prepare.coder),
            EXECUTE        (10, Requests.Execute.coder),
            REGISTER       (11, Requests.Register.coder),
            BATCH          (13, Requests.Batch.coder),
            AUTH_RESPONSE  (15, Requests.AuthResponse.coder);

            public final int opcode;
            public final Coder<?> coder;

            private Type(int opcode, Coder<?> coder) {
                this.opcode = opcode;
                this.coder = coder;
            }
        }

        public final Type type;
        protected boolean tracingRequested;

        protected Request(Type type) {
            this.type = type;
        }

        public void setTracingRequested() {
            this.tracingRequested = true;
        }

        public boolean isTracingRequested() {
            return tracingRequested;
        }
    }

    public static abstract class Response extends Message {

        public enum Type {
            ERROR          (0,  Responses.Error.decoder),
            READY          (2,  Responses.Ready.decoder),
            AUTHENTICATE   (3,  Responses.Authenticate.decoder),
            SUPPORTED      (6,  Responses.Supported.decoder),
            RESULT         (8,  Responses.Result.decoder),
            EVENT          (12, Responses.Event.decoder),
            AUTH_CHALLENGE (14, Responses.AuthChallenge.decoder),
            AUTH_SUCCESS   (16, Responses.AuthSuccess.decoder);

            public final int opcode;
            public final Decoder<?> decoder;

            private static final Type[] opcodeIdx;
            static {
                int maxOpcode = -1;
                for (Type type : Type.values())
                    maxOpcode = Math.max(maxOpcode, type.opcode);
                opcodeIdx = new Type[maxOpcode + 1];
                for (Type type : Type.values()) {
                    if (opcodeIdx[type.opcode] != null)
                        throw new IllegalStateException("Duplicate opcode");
                    opcodeIdx[type.opcode] = type;
                }
            }

            private Type(int opcode, Decoder<?> decoder) {
                this.opcode = opcode;
                this.decoder = decoder;
            }

            public static Type fromOpcode(int opcode) {
                Type t = opcodeIdx[opcode];
                if (t == null)
                    throw new DriverInternalError(String.format("Unknown response opcode %d", opcode));
                return t;
            }
        }

        public final Type type;
        protected UUID tracingId;

        protected Response(Type type) {
            this.type = type;
        }

        public Response setTracingId(UUID tracingId) {
            this.tracingId = tracingId;
            return this;
        }

        public UUID getTracingId() {
            return tracingId;
        }
    }

    public static class ProtocolDecoder extends OneToOneDecoder {

        public Object decode(ChannelHandlerContext ctx, Channel channel, Object msg) {
            assert msg instanceof Frame : "Expecting frame, got " + msg;

            Frame frame = (Frame)msg;
            boolean isTracing = frame.header.flags.contains(Frame.Header.Flag.TRACING);
            UUID tracingId = isTracing ? CBUtil.readUUID(frame.body) : null;

            Response response = Response.Type.fromOpcode(frame.header.opcode).decoder.decode(frame.body);
            return response.setTracingId(tracingId).setStreamId(frame.header.streamId);
        }
    }

    public static class ProtocolEncoder extends OneToOneEncoder {

        @SuppressWarnings("unchecked")
        public Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) {
            assert msg instanceof Request : "Expecting request, got " + msg;

            Request request = (Request)msg;

            EnumSet<Frame.Header.Flag> flags = EnumSet.noneOf(Frame.Header.Flag.class);
            if (request.isTracingRequested())
                flags.add(Frame.Header.Flag.TRACING);

            Coder<Request> coder = (Coder<Request>)request.type.coder;
            ChannelBuffer body = ChannelBuffers.buffer(coder.encodedSize(request));
            coder.encode(request, body);

            return Frame.create(request.type.opcode, request.getStreamId(), flags, body);
        }
    }
}
