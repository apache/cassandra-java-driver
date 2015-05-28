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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.DriverInternalError;

/**
 * A message from the CQL binary protocol.
 */
abstract class Message {

    protected static final Logger logger = LoggerFactory.getLogger(Message.class);

    public interface Coder<R extends Request> {
        public void encode(R request, ByteBuf dest, ProtocolVersion version);
        public int encodedSize(R request, ProtocolVersion version);
    }

    public interface Decoder<R extends Response> {
        public R decode(ByteBuf body, ProtocolVersion version);
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
            STARTUP        (1, Requests.Startup.coder),
            CREDENTIALS    (4, Requests.Credentials.coder),
            OPTIONS        (5, Requests.Options.coder),
            QUERY          (7, Requests.Query.coder),
            PREPARE        (9, Requests.Prepare.coder),
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

        ConsistencyLevel consistency() {
            switch (this.type) {
                case QUERY:   return ((Requests.Query)this).options.consistency;
                case EXECUTE: return ((Requests.Execute)this).options.consistency;
                case BATCH:   return ((Requests.Batch)this).options.consistency;
                default:      return null;
            }
        }

        ConsistencyLevel serialConsistency() {
            switch (this.type) {
                case QUERY:   return ((Requests.Query)this).options.serialConsistency;
                case EXECUTE: return ((Requests.Execute)this).options.serialConsistency;
                case BATCH:   return ((Requests.Batch)this).options.serialConsistency;
                default:      return null;
            }
        }

        long defaultTimestamp() {
            switch (this.type) {
                case QUERY:   return ((Requests.Query)this).options.defaultTimestamp;
                case EXECUTE: return ((Requests.Execute)this).options.defaultTimestamp;
                case BATCH:   return ((Requests.Batch)this).options.defaultTimestamp;
                default:      return 0;
            }
        }

        ByteBuffer pagingState() {
            switch (this.type) {
                case QUERY:   return ((Requests.Query)this).options.pagingState;
                case EXECUTE: return ((Requests.Execute)this).options.pagingState;
                default:      return null;
            }
        }
    }

    public static abstract class Response extends Message {

        public enum Type {
            ERROR          (0, Responses.Error.decoder),
            READY          (2, Responses.Ready.decoder),
            AUTHENTICATE   (3, Responses.Authenticate.decoder),
            SUPPORTED      (6, Responses.Supported.decoder),
            RESULT         (8, Responses.Result.decoder),
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
                if (opcode < 0 || opcode >= opcodeIdx.length)
                    throw new DriverInternalError(String.format("Unknown response opcode %d", opcode));
                Type t = opcodeIdx[opcode];
                if (t == null)
                    throw new DriverInternalError(String.format("Unknown response opcode %d", opcode));
                return t;
            }
        }

        public final Type type;
        protected volatile UUID tracingId;
        protected volatile List<String> warnings;

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

        public Response setWarnings(List<String> warnings) {
            this.warnings = warnings;
            return this;
        }
    }

    @ChannelHandler.Sharable
    public static class ProtocolDecoder extends MessageToMessageDecoder<Frame> {

        @Override
        protected void decode(ChannelHandlerContext ctx, Frame frame, List<Object> out) throws Exception {
            boolean isTracing = frame.header.flags.contains(Frame.Header.Flag.TRACING);
            UUID tracingId = isTracing ? CBUtil.readUUID(frame.body) : null;

            boolean hasWarnings = frame.header.flags.contains(Frame.Header.Flag.WARNING);
            List<String> warnings = hasWarnings ? CBUtil.readStringList(frame.body) : Collections.<String>emptyList();

            try {
                Response response = Response.Type.fromOpcode(frame.header.opcode).decoder.decode(frame.body, frame.header.version);
                response.setTracingId(tracingId).setWarnings(warnings).setStreamId(frame.header.streamId);
                out.add(response);
            } finally {
                frame.body.release();
            }
        }
    }

    @ChannelHandler.Sharable
    public static class ProtocolEncoder extends MessageToMessageEncoder<Request> {

        private final ProtocolVersion protocolVersion;

        public ProtocolEncoder(ProtocolVersion version) {
            this.protocolVersion = version;
        }

        @Override
        protected void encode(ChannelHandlerContext ctx, Request request, List<Object> out) throws Exception {
            EnumSet<Frame.Header.Flag> flags = EnumSet.noneOf(Frame.Header.Flag.class);
            if (request.isTracingRequested())
                flags.add(Frame.Header.Flag.TRACING);

            @SuppressWarnings("unchecked")
            Coder<Request> coder = (Coder<Request>)request.type.coder;
            ByteBuf body = ctx.alloc().buffer(coder.encodedSize(request, protocolVersion));
            coder.encode(request, body, protocolVersion);

            out.add(Frame.create(protocolVersion, request.type.opcode, request.getStreamId(), flags, body));
        }
    }
}
