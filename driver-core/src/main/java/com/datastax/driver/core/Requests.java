/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

class Requests {

    private Requests() {
    }

    static class Startup extends Message.Request {
        private static final String CQL_VERSION_OPTION = "CQL_VERSION";
        private static final String CQL_VERSION = "3.0.0";

        static final String COMPRESSION_OPTION = "COMPRESSION";

        static final Message.Coder<Startup> coder = new Message.Coder<Startup>() {
            @Override
            public void encode(Startup msg, ByteBuf dest, ProtocolVersion version) {
                CBUtil.writeStringMap(msg.options, dest);
            }

            @Override
            public int encodedSize(Startup msg, ProtocolVersion version) {
                return CBUtil.sizeOfStringMap(msg.options);
            }
        };

        private final Map<String, String> options;
        private final ProtocolOptions.Compression compression;

        Startup(ProtocolOptions.Compression compression) {
            super(Message.Request.Type.STARTUP);
            this.compression = compression;

            ImmutableMap.Builder<String, String> map = new ImmutableMap.Builder<String, String>();
            map.put(CQL_VERSION_OPTION, CQL_VERSION);
            if (compression != ProtocolOptions.Compression.NONE)
                map.put(COMPRESSION_OPTION, compression.toString());
            this.options = map.build();
        }

        @Override
        protected Request copyInternal() {
            return new Startup(compression);
        }

        @Override
        public String toString() {
            return "STARTUP " + options;
        }
    }

    // Only for protocol v1
    static class Credentials extends Message.Request {

        static final Message.Coder<Credentials> coder = new Message.Coder<Credentials>() {

            @Override
            public void encode(Credentials msg, ByteBuf dest, ProtocolVersion version) {
                assert version == ProtocolVersion.V1;
                CBUtil.writeStringMap(msg.credentials, dest);
            }

            @Override
            public int encodedSize(Credentials msg, ProtocolVersion version) {
                assert version == ProtocolVersion.V1;
                return CBUtil.sizeOfStringMap(msg.credentials);
            }
        };

        private final Map<String, String> credentials;

        Credentials(Map<String, String> credentials) {
            super(Message.Request.Type.CREDENTIALS);
            this.credentials = credentials;
        }

        @Override
        protected Request copyInternal() {
            return new Credentials(credentials);
        }
    }

    static class Options extends Message.Request {

        static final Message.Coder<Options> coder = new Message.Coder<Options>() {
            @Override
            public void encode(Options msg, ByteBuf dest, ProtocolVersion version) {
            }

            @Override
            public int encodedSize(Options msg, ProtocolVersion version) {
                return 0;
            }
        };

        Options() {
            super(Message.Request.Type.OPTIONS);
        }

        @Override
        protected Request copyInternal() {
            return new Options();
        }

        @Override
        public String toString() {
            return "OPTIONS";
        }
    }

    static class Query extends Message.Request {

        static final Message.Coder<Query> coder = new Message.Coder<Query>() {
            @Override
            public void encode(Query msg, ByteBuf dest, ProtocolVersion version) {
                CBUtil.writeLongString(msg.query, dest);
                msg.options.encode(dest, version);
            }

            @Override
            public int encodedSize(Query msg, ProtocolVersion version) {
                return CBUtil.sizeOfLongString(msg.query)
                        + msg.options.encodedSize(version);
            }
        };

        final String query;
        final QueryProtocolOptions options;

        Query(String query) {
            this(query, QueryProtocolOptions.DEFAULT, false);
        }

        Query(String query, QueryProtocolOptions options, boolean tracingRequested) {
            super(Type.QUERY, tracingRequested);
            this.query = query;
            this.options = options;
        }

        @Override
        protected Request copyInternal() {
            return new Query(this.query, options, isTracingRequested());
        }

        @Override
        protected Request copyInternal(ConsistencyLevel newConsistencyLevel) {
            return new Query(this.query, options.copy(newConsistencyLevel), isTracingRequested());
        }

        @Override
        public String toString() {
            return "QUERY " + query + '(' + options + ')';
        }
    }

    static class Execute extends Message.Request {

        static final Message.Coder<Execute> coder = new Message.Coder<Execute>() {
            @Override
            public void encode(Execute msg, ByteBuf dest, ProtocolVersion version) {
                CBUtil.writeBytes(msg.statementId.bytes, dest);
                msg.options.encode(dest, version);
            }

            @Override
            public int encodedSize(Execute msg, ProtocolVersion version) {
                return CBUtil.sizeOfBytes(msg.statementId.bytes)
                        + msg.options.encodedSize(version);
            }
        };

        final MD5Digest statementId;
        final QueryProtocolOptions options;

        Execute(MD5Digest statementId, QueryProtocolOptions options, boolean tracingRequested) {
            super(Message.Request.Type.EXECUTE, tracingRequested);
            this.statementId = statementId;
            this.options = options;
        }

        @Override
        protected Request copyInternal() {
            return new Execute(statementId, options, isTracingRequested());
        }

        @Override
        protected Request copyInternal(ConsistencyLevel newConsistencyLevel) {
            return new Execute(statementId, options.copy(newConsistencyLevel), isTracingRequested());
        }

        @Override
        public String toString() {
            return "EXECUTE " + statementId + " (" + options + ')';
        }
    }

    enum QueryFlag {
        // The order of that enum matters!!
        VALUES,
        SKIP_METADATA,
        PAGE_SIZE,
        PAGING_STATE,
        SERIAL_CONSISTENCY,
        DEFAULT_TIMESTAMP,
        VALUE_NAMES;

        static EnumSet<QueryFlag> deserialize(int flags) {
            EnumSet<QueryFlag> set = EnumSet.noneOf(QueryFlag.class);
            QueryFlag[] values = QueryFlag.values();
            for (int n = 0; n < values.length; n++) {
                if ((flags & (1 << n)) != 0)
                    set.add(values[n]);
            }
            return set;
        }

        static void serialize(EnumSet<QueryFlag> flags, ByteBuf dest, ProtocolVersion version) {
            int i = 0;
            for (QueryFlag flag : flags)
                i |= 1 << flag.ordinal();
            if (version.compareTo(ProtocolVersion.V5) >= 0) {
                dest.writeInt(i);
            } else {
                dest.writeByte((byte) i);
            }
        }

        static int serializedSize(ProtocolVersion version) {
            return version.compareTo(ProtocolVersion.V5) >= 0 ? 4 : 1;
        }
    }

    static class QueryProtocolOptions {

        static final QueryProtocolOptions DEFAULT = new QueryProtocolOptions(
                Message.Request.Type.QUERY,
                ConsistencyLevel.ONE,
                Collections.<ByteBuffer>emptyList(),
                Collections.<String, ByteBuffer>emptyMap(),
                false,
                -1,
                null,
                ConsistencyLevel.SERIAL, Long.MIN_VALUE);

        private final EnumSet<QueryFlag> flags = EnumSet.noneOf(QueryFlag.class);
        private final Message.Request.Type requestType;
        final ConsistencyLevel consistency;
        final List<ByteBuffer> positionalValues;
        final Map<String, ByteBuffer> namedValues;
        final boolean skipMetadata;
        final int pageSize;
        final ByteBuffer pagingState;
        final ConsistencyLevel serialConsistency;
        final long defaultTimestamp;

        QueryProtocolOptions(Message.Request.Type requestType,
                             ConsistencyLevel consistency,
                             List<ByteBuffer> positionalValues,
                             Map<String, ByteBuffer> namedValues,
                             boolean skipMetadata,
                             int pageSize,
                             ByteBuffer pagingState,
                             ConsistencyLevel serialConsistency,
                             long defaultTimestamp) {

            Preconditions.checkArgument(positionalValues.isEmpty() || namedValues.isEmpty());

            this.requestType = requestType;
            this.consistency = consistency;
            this.positionalValues = positionalValues;
            this.namedValues = namedValues;
            this.skipMetadata = skipMetadata;
            this.pageSize = pageSize;
            this.pagingState = pagingState;
            this.serialConsistency = serialConsistency;
            this.defaultTimestamp = defaultTimestamp;

            // Populate flags
            if (!positionalValues.isEmpty())
                flags.add(QueryFlag.VALUES);
            if (!namedValues.isEmpty()) {
                flags.add(QueryFlag.VALUES);
                flags.add(QueryFlag.VALUE_NAMES);
            }
            if (skipMetadata)
                flags.add(QueryFlag.SKIP_METADATA);
            if (pageSize >= 0)
                flags.add(QueryFlag.PAGE_SIZE);
            if (pagingState != null)
                flags.add(QueryFlag.PAGING_STATE);
            if (serialConsistency != ConsistencyLevel.SERIAL)
                flags.add(QueryFlag.SERIAL_CONSISTENCY);
            if (defaultTimestamp != Long.MIN_VALUE)
                flags.add(QueryFlag.DEFAULT_TIMESTAMP);
        }

        QueryProtocolOptions copy(ConsistencyLevel newConsistencyLevel) {
            return new QueryProtocolOptions(requestType, newConsistencyLevel, positionalValues, namedValues, skipMetadata, pageSize, pagingState, serialConsistency, defaultTimestamp);
        }

        void encode(ByteBuf dest, ProtocolVersion version) {
            switch (version) {
                case V1:
                    // only EXECUTE messages have variables in V1, and their list must be written
                    // even if it is empty; and they are never named
                    if (requestType == Message.Request.Type.EXECUTE)
                        CBUtil.writeValueList(positionalValues, dest);
                    CBUtil.writeConsistencyLevel(consistency, dest);
                    break;
                case V2:
                case V3:
                case V4:
                case V5:
                    CBUtil.writeConsistencyLevel(consistency, dest);
                    QueryFlag.serialize(flags, dest, version);
                    if (flags.contains(QueryFlag.VALUES)) {
                        if (flags.contains(QueryFlag.VALUE_NAMES)) {
                            assert version.compareTo(ProtocolVersion.V3) >= 0;
                            CBUtil.writeNamedValueList(namedValues, dest);
                        } else {
                            CBUtil.writeValueList(positionalValues, dest);
                        }
                    }
                    if (flags.contains(QueryFlag.PAGE_SIZE))
                        dest.writeInt(pageSize);
                    if (flags.contains(QueryFlag.PAGING_STATE))
                        CBUtil.writeValue(pagingState, dest);
                    if (flags.contains(QueryFlag.SERIAL_CONSISTENCY))
                        CBUtil.writeConsistencyLevel(serialConsistency, dest);
                    if (version.compareTo(ProtocolVersion.V3) >= 0 && flags.contains(QueryFlag.DEFAULT_TIMESTAMP))
                        dest.writeLong(defaultTimestamp);
                    break;
                default:
                    throw version.unsupported();
            }
        }

        int encodedSize(ProtocolVersion version) {
            switch (version) {
                case V1:
                    // only EXECUTE messages have variables in V1, and their list must be written
                    // even if it is empty; and they are never named
                    return (requestType == Message.Request.Type.EXECUTE ? CBUtil.sizeOfValueList(positionalValues) : 0)
                            + CBUtil.sizeOfConsistencyLevel(consistency);
                case V2:
                case V3:
                case V4:
                case V5:
                    int size = 0;
                    size += CBUtil.sizeOfConsistencyLevel(consistency);
                    size += QueryFlag.serializedSize(version);
                    if (flags.contains(QueryFlag.VALUES)) {
                        if (flags.contains(QueryFlag.VALUE_NAMES)) {
                            assert version.compareTo(ProtocolVersion.V3) >= 0;
                            size += CBUtil.sizeOfNamedValueList(namedValues);
                        } else {
                            size += CBUtil.sizeOfValueList(positionalValues);
                        }
                    }
                    if (flags.contains(QueryFlag.PAGE_SIZE))
                        size += 4;
                    if (flags.contains(QueryFlag.PAGING_STATE))
                        size += CBUtil.sizeOfValue(pagingState);
                    if (flags.contains(QueryFlag.SERIAL_CONSISTENCY))
                        size += CBUtil.sizeOfConsistencyLevel(serialConsistency);
                    if (version == ProtocolVersion.V3 && flags.contains(QueryFlag.DEFAULT_TIMESTAMP))
                        size += 8;
                    return size;
                default:
                    throw version.unsupported();
            }
        }

        @Override
        public String toString() {
            return String.format("[cl=%s, positionalVals=%s, namedVals=%s, skip=%b, psize=%d, state=%s, serialCl=%s]",
                    consistency, positionalValues, namedValues, skipMetadata, pageSize, pagingState, serialConsistency);
        }
    }

    static class Batch extends Message.Request {

        static final Message.Coder<Batch> coder = new Message.Coder<Batch>() {
            @Override
            public void encode(Batch msg, ByteBuf dest, ProtocolVersion version) {
                int queries = msg.queryOrIdList.size();
                assert queries <= 0xFFFF;

                dest.writeByte(fromType(msg.type));
                dest.writeShort(queries);

                for (int i = 0; i < queries; i++) {
                    Object q = msg.queryOrIdList.get(i);
                    dest.writeByte((byte) (q instanceof String ? 0 : 1));
                    if (q instanceof String)
                        CBUtil.writeLongString((String) q, dest);
                    else
                        CBUtil.writeBytes(((MD5Digest) q).bytes, dest);

                    CBUtil.writeValueList(msg.values.get(i), dest);
                }

                msg.options.encode(dest, version);
            }

            @Override
            public int encodedSize(Batch msg, ProtocolVersion version) {
                int size = 3; // type + nb queries
                for (int i = 0; i < msg.queryOrIdList.size(); i++) {
                    Object q = msg.queryOrIdList.get(i);
                    size += 1 + (q instanceof String
                            ? CBUtil.sizeOfLongString((String) q)
                            : CBUtil.sizeOfBytes(((MD5Digest) q).bytes));

                    size += CBUtil.sizeOfValueList(msg.values.get(i));
                }
                size += msg.options.encodedSize(version);
                return size;
            }

            private byte fromType(BatchStatement.Type type) {
                switch (type) {
                    case LOGGED:
                        return 0;
                    case UNLOGGED:
                        return 1;
                    case COUNTER:
                        return 2;
                    default:
                        throw new AssertionError();
                }
            }
        };

        final BatchStatement.Type type;
        final List<Object> queryOrIdList;
        final List<List<ByteBuffer>> values;
        final BatchProtocolOptions options;

        Batch(BatchStatement.Type type, List<Object> queryOrIdList, List<List<ByteBuffer>> values, BatchProtocolOptions options, boolean tracingRequested) {
            super(Message.Request.Type.BATCH, tracingRequested);
            this.type = type;
            this.queryOrIdList = queryOrIdList;
            this.values = values;
            this.options = options;
        }

        @Override
        protected Request copyInternal() {
            return new Batch(type, queryOrIdList, values, options, isTracingRequested());
        }

        @Override
        protected Request copyInternal(ConsistencyLevel newConsistencyLevel) {
            return new Batch(type, queryOrIdList, values, options.copy(newConsistencyLevel), isTracingRequested());
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("BATCH of [");
            for (int i = 0; i < queryOrIdList.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(queryOrIdList.get(i)).append(" with ").append(values.get(i).size()).append(" values");
            }
            sb.append("] with options ").append(options);
            return sb.toString();
        }
    }

    static class BatchProtocolOptions {
        private final EnumSet<QueryFlag> flags = EnumSet.noneOf(QueryFlag.class);
        final ConsistencyLevel consistency;
        final ConsistencyLevel serialConsistency;
        final long defaultTimestamp;

        BatchProtocolOptions(ConsistencyLevel consistency, ConsistencyLevel serialConsistency, long defaultTimestamp) {
            this.consistency = consistency;
            this.serialConsistency = serialConsistency;
            this.defaultTimestamp = defaultTimestamp;

            if (serialConsistency != ConsistencyLevel.SERIAL)
                flags.add(QueryFlag.SERIAL_CONSISTENCY);
            if (defaultTimestamp != Long.MIN_VALUE)
                flags.add(QueryFlag.DEFAULT_TIMESTAMP);
        }

        BatchProtocolOptions copy(ConsistencyLevel newConsistencyLevel) {
            return new BatchProtocolOptions(newConsistencyLevel, serialConsistency, defaultTimestamp);
        }

        void encode(ByteBuf dest, ProtocolVersion version) {
            switch (version) {
                case V2:
                    CBUtil.writeConsistencyLevel(consistency, dest);
                    break;
                case V3:
                case V4:
                case V5:
                    CBUtil.writeConsistencyLevel(consistency, dest);
                    QueryFlag.serialize(flags, dest, version);
                    if (flags.contains(QueryFlag.SERIAL_CONSISTENCY))
                        CBUtil.writeConsistencyLevel(serialConsistency, dest);
                    if (flags.contains(QueryFlag.DEFAULT_TIMESTAMP))
                        dest.writeLong(defaultTimestamp);
                    break;
                default:
                    throw version.unsupported();
            }
        }

        int encodedSize(ProtocolVersion version) {
            switch (version) {
                case V2:
                    return CBUtil.sizeOfConsistencyLevel(consistency);
                case V3:
                case V4:
                case V5:
                    int size = 0;
                    size += CBUtil.sizeOfConsistencyLevel(consistency);
                    size += QueryFlag.serializedSize(version);
                    if (flags.contains(QueryFlag.SERIAL_CONSISTENCY))
                        size += CBUtil.sizeOfConsistencyLevel(serialConsistency);
                    if (flags.contains(QueryFlag.DEFAULT_TIMESTAMP))
                        size += 8;
                    return size;
                default:
                    throw version.unsupported();
            }
        }

        @Override
        public String toString() {
            return String.format("[cl=%s, serialCl=%s, defaultTs=%d]",
                    consistency, serialConsistency, defaultTimestamp);
        }
    }

    static class Prepare extends Message.Request {

        static final Message.Coder<Prepare> coder = new Message.Coder<Prepare>() {

            @Override
            public void encode(Prepare msg, ByteBuf dest, ProtocolVersion version) {
                CBUtil.writeLongString(msg.query, dest);

                if (version.compareTo(ProtocolVersion.V5) >= 0) {
                    // Write empty flags for now, to communicate that no keyspace is being set.
                    dest.writeInt(0);
                }
            }

            @Override
            public int encodedSize(Prepare msg, ProtocolVersion version) {
                return CBUtil.sizeOfLongString(msg.query);
            }
        };

        private final String query;

        Prepare(String query) {
            super(Message.Request.Type.PREPARE);
            this.query = query;
        }

        @Override
        protected Request copyInternal() {
            return new Prepare(query);
        }

        @Override
        public String toString() {
            return "PREPARE " + query;
        }
    }

    static class Register extends Message.Request {

        static final Message.Coder<Register> coder = new Message.Coder<Register>() {
            @Override
            public void encode(Register msg, ByteBuf dest, ProtocolVersion version) {
                dest.writeShort(msg.eventTypes.size());
                for (ProtocolEvent.Type type : msg.eventTypes)
                    CBUtil.writeEnumValue(type, dest);
            }

            @Override
            public int encodedSize(Register msg, ProtocolVersion version) {
                int size = 2;
                for (ProtocolEvent.Type type : msg.eventTypes)
                    size += CBUtil.sizeOfEnumValue(type);
                return size;
            }
        };

        private final List<ProtocolEvent.Type> eventTypes;

        Register(List<ProtocolEvent.Type> eventTypes) {
            super(Message.Request.Type.REGISTER);
            this.eventTypes = eventTypes;
        }

        @Override
        protected Request copyInternal() {
            return new Register(eventTypes);
        }

        @Override
        public String toString() {
            return "REGISTER " + eventTypes;
        }
    }

    static class AuthResponse extends Message.Request {

        static final Message.Coder<AuthResponse> coder = new Message.Coder<AuthResponse>() {

            @Override
            public void encode(AuthResponse response, ByteBuf dest, ProtocolVersion version) {
                CBUtil.writeValue(response.token, dest);
            }

            @Override
            public int encodedSize(AuthResponse response, ProtocolVersion version) {
                return CBUtil.sizeOfValue(response.token);
            }
        };

        private final byte[] token;

        AuthResponse(byte[] token) {
            super(Message.Request.Type.AUTH_RESPONSE);
            this.token = token;
        }

        @Override
        protected Request copyInternal() {
            return new AuthResponse(token);
        }
    }
}
