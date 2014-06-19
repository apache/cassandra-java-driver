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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.jboss.netty.buffer.ChannelBuffer;

class Requests {

    private Requests() {}

    public static class Startup extends Message.Request {
        private static final String CQL_VERSION_OPTION = "CQL_VERSION";
        private static final String CQL_VERSION = "3.0.0";

        public static final String COMPRESSION_OPTION = "COMPRESSION";

        public static final Message.Coder<Startup> coder = new Message.Coder<Startup>() {
            @Override public void encode(Startup msg, ChannelBuffer dest) {
                CBUtil.writeStringMap(msg.options, dest);
            }

            @Override public int encodedSize(Startup msg) {
                return CBUtil.sizeOfStringMap(msg.options);
            }
        };

        private final Map<String, String> options;

        public Startup(ProtocolOptions.Compression compression) {
            super(Message.Request.Type.STARTUP);

            ImmutableMap.Builder<String, String> map = new ImmutableMap.Builder<String, String>();
            map.put(CQL_VERSION_OPTION, CQL_VERSION);
            if (compression != ProtocolOptions.Compression.NONE)
                map.put(COMPRESSION_OPTION, compression.toString());
            this.options = map.build();
        }

        @Override
        public String toString() {
            return "STARTUP " + options;
        }
    }

    // Only for protocol v1
    public static class Credentials extends Message.Request {

        public static final Message.Coder<Credentials> coder = new Message.Coder<Credentials>() {

            @Override public void encode(Credentials msg, ChannelBuffer dest) {
                CBUtil.writeStringMap(msg.credentials, dest);
            }

            @Override public int encodedSize(Credentials msg) {
                return CBUtil.sizeOfStringMap(msg.credentials);
            }
        };

        private final Map<String, String> credentials;

        public Credentials(Map<String, String> credentials) {
            super(Message.Request.Type.CREDENTIALS);
            this.credentials = credentials;
        }
    }

    public static class Options extends Message.Request {

        public static final Message.Coder<Options> coder = new Message.Coder<Options>()
        {
            @Override public void encode(Options msg, ChannelBuffer dest) {}

            @Override public int encodedSize(Options msg) {
                return 0;
            }
        };

        public Options() {
            super(Message.Request.Type.OPTIONS);
        }

        @Override
        public String toString() {
            return "OPTIONS";
        }
    }

    public static class Query extends Message.Request {

        public static final Message.Coder<Query> coderV1 = new Message.Coder<Query>() {
            @Override public void encode(Query msg, ChannelBuffer dest) {
                CBUtil.writeLongString(msg.query, dest);
                CBUtil.writeConsistencyLevel(msg.options.consistency, dest);
            }

            @Override public int encodedSize(Query msg) {
                return CBUtil.sizeOfLongString(msg.query)
                     + CBUtil.sizeOfConsistencyLevel(msg.options.consistency);
            }
        };

        public static final Message.Coder<Query> coderV2 = new Message.Coder<Query>() {
            @Override public void encode(Query msg, ChannelBuffer dest) {
                CBUtil.writeLongString(msg.query, dest);
                msg.options.encodeV2(dest);
            }

            @Override public int encodedSize(Query msg) {
                return CBUtil.sizeOfLongString(msg.query)
                    + msg.options.encodedSizeV2();
            }
        };

        public static final Message.Coder<Query> coderV3 = new Message.Coder<Query>() {
            @Override public void encode(Query msg, ChannelBuffer dest) {
                CBUtil.writeLongString(msg.query, dest);
                msg.options.encodeV3(dest);
            }

            @Override public int encodedSize(Query msg) {
                return CBUtil.sizeOfLongString(msg.query)
                    + msg.options.encodedSizeV3();
            }
        };

        public final String query;
        public final QueryProtocolOptions options;

        public Query(String query) {
            this(query, QueryProtocolOptions.DEFAULT);
        }

        public Query(String query, QueryProtocolOptions options) {
            super(Type.QUERY);
            this.query = query;
            this.options = options;
        }

        @Override
        public String toString() {
            return "QUERY " + query + '(' + options + ')';
        }
    }

    public static class Execute extends Message.Request {

        public static final Message.Coder<Execute> coderV1 = new Message.Coder<Execute>() {
            @Override public void encode(Execute msg, ChannelBuffer dest) {
                CBUtil.writeBytes(msg.statementId.bytes, dest);
                CBUtil.writeValueList(msg.options.values, dest);
                CBUtil.writeConsistencyLevel(msg.options.consistency, dest);
            }

            @Override public int encodedSize(Execute msg) {
                return CBUtil.sizeOfBytes(msg.statementId.bytes)
                     + CBUtil.sizeOfValueList(msg.options.values)
                     + CBUtil.sizeOfConsistencyLevel(msg.options.consistency);
            }
        };

        public static final Message.Coder<Execute> coderV2 = new Message.Coder<Execute>() {
            @Override public void encode(Execute msg, ChannelBuffer dest) {
                CBUtil.writeBytes(msg.statementId.bytes, dest);
                msg.options.encodeV2(dest);
            }

            @Override public int encodedSize(Execute msg) {
                return CBUtil.sizeOfBytes(msg.statementId.bytes)
                    + msg.options.encodedSizeV2();
            }
        };

        public static final Message.Coder<Execute> coderV3 = new Message.Coder<Execute>() {
            @Override public void encode(Execute msg, ChannelBuffer dest) {
                CBUtil.writeBytes(msg.statementId.bytes, dest);
                msg.options.encodeV3(dest);
            }

            @Override public int encodedSize(Execute msg) {
                return CBUtil.sizeOfBytes(msg.statementId.bytes)
                    + msg.options.encodedSizeV3();
            }
        };

        public final MD5Digest statementId;
        public final QueryProtocolOptions options;

        public Execute(MD5Digest statementId, QueryProtocolOptions options) {
            super(Message.Request.Type.EXECUTE);
            this.statementId = statementId;
            this.options = options;
        }

        @Override
        public String toString() {
            return "EXECUTE " + statementId + " (" + options + ')';
        }
    }

    public static class QueryProtocolOptions {

        private enum Flag {
            // The order of that enum matters!!
            VALUES,
            SKIP_METADATA,
            PAGE_SIZE,
            PAGING_STATE,
            SERIAL_CONSISTENCY,
            DEFAULT_TIMESTAMP,
            VALUE_NAMES;

            public static EnumSet<Flag> deserialize(int flags) {
                EnumSet<Flag> set = EnumSet.noneOf(Flag.class);
                Flag[] values = Flag.values();
                for (int n = 0; n < values.length; n++)
                {
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

        public static final QueryProtocolOptions DEFAULT = new QueryProtocolOptions(ConsistencyLevel.ONE,
                                                                                    Collections.<ByteBuffer>emptyList(),
                                                                                    false,
                                                                                    -1,
                                                                                    null,
                                                                                    ConsistencyLevel.SERIAL,
                                                                                    0L);

        private final EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
        public final ConsistencyLevel consistency;
        public final List<ByteBuffer> values;
        public final boolean skipMetadata;
        public final int pageSize;
        public final ByteBuffer pagingState;
        public final ConsistencyLevel serialConsistency;
        public long defaultTimestamp;

        public QueryProtocolOptions(ConsistencyLevel consistency,
                                    List<ByteBuffer> values,
                                    boolean skipMetadata,
                                    int pageSize,
                                    ByteBuffer pagingState,
                                    ConsistencyLevel serialConsistency,
                                    long defaultTimestamp) {

            this.consistency = consistency;
            this.values = values;
            this.skipMetadata = skipMetadata;
            this.pageSize = pageSize;
            this.pagingState = pagingState;
            this.serialConsistency = serialConsistency;
            this.defaultTimestamp = defaultTimestamp;

            // Populate flags
            if (!values.isEmpty())
                flags.add(Flag.VALUES);
            if (skipMetadata)
                flags.add(Flag.SKIP_METADATA);
            if (pageSize >= 0)
                flags.add(Flag.PAGE_SIZE);
            if (pagingState != null)
                flags.add(Flag.PAGING_STATE);
            if (serialConsistency != ConsistencyLevel.SERIAL)
                flags.add(Flag.SERIAL_CONSISTENCY);
            if (defaultTimestamp != 0L)
                flags.add(Flag.DEFAULT_TIMESTAMP);
        }

        public void encodeV2(ChannelBuffer dest) {
            CBUtil.writeConsistencyLevel(consistency, dest);

            dest.writeByte((byte)Flag.serialize(flags));

            if (flags.contains(Flag.VALUES))
                CBUtil.writeValueList(values, dest);
            if (flags.contains(Flag.PAGE_SIZE))
                dest.writeInt(pageSize);
            if (flags.contains(Flag.PAGING_STATE))
                CBUtil.writeValue(pagingState, dest);
            if (flags.contains(Flag.SERIAL_CONSISTENCY))
                CBUtil.writeConsistencyLevel(serialConsistency, dest);
        }

        public int encodedSizeV2() {
            int size = CBUtil.sizeOfConsistencyLevel(consistency) + 1;

            if (flags.contains(Flag.VALUES))
                size += CBUtil.sizeOfValueList(values);
            if (flags.contains(Flag.PAGE_SIZE))
                size += 4;
            if (flags.contains(Flag.PAGING_STATE))
                size += CBUtil.sizeOfValue(pagingState);
            if (flags.contains(Flag.SERIAL_CONSISTENCY))
                size += CBUtil.sizeOfConsistencyLevel(serialConsistency);
            return size;
        }

        public void encodeV3(ChannelBuffer dest) {
            CBUtil.writeConsistencyLevel(consistency, dest);

            dest.writeByte((byte)Flag.serialize(flags));

            if (flags.contains(Flag.VALUES))
                CBUtil.writeValueList(values, dest);
            if (flags.contains(Flag.PAGE_SIZE))
                dest.writeInt(pageSize);
            if (flags.contains(Flag.PAGING_STATE))
                CBUtil.writeValue(pagingState, dest);
            if (flags.contains(Flag.SERIAL_CONSISTENCY))
                CBUtil.writeConsistencyLevel(serialConsistency, dest);
            if (flags.contains(Flag.DEFAULT_TIMESTAMP))
                dest.writeLong(defaultTimestamp);
        }

        public int encodedSizeV3() {
            int size = CBUtil.sizeOfConsistencyLevel(consistency) + 1;

            if (flags.contains(Flag.VALUES))
                size += CBUtil.sizeOfValueList(values);
            if (flags.contains(Flag.PAGE_SIZE))
                size += 4;
            if (flags.contains(Flag.PAGING_STATE))
                size += CBUtil.sizeOfValue(pagingState);
            if (flags.contains(Flag.SERIAL_CONSISTENCY))
                size += CBUtil.sizeOfConsistencyLevel(serialConsistency);
            if (flags.contains(Flag.DEFAULT_TIMESTAMP))
                size += 4;
            return size;
        }

        @Override
        public String toString() {
            return String.format("[cl=%s, vals=%s, skip=%b, psize=%d, state=%s, serialCl=%s]", consistency, values, skipMetadata, pageSize, pagingState, serialConsistency);
        }
    }

    public static class Batch extends Message.Request {

        public static final Message.Coder<Batch> coderV2 = new Message.Coder<Batch>() {
            @Override public void encode(Batch msg, ChannelBuffer dest) {
                int queries = msg.queryOrIdList.size();
                assert queries <= 0xFFFF;

                dest.writeByte(fromType(msg.type));
                dest.writeShort(queries);

                for (int i = 0; i < queries; i++) {
                    Object q = msg.queryOrIdList.get(i);
                    dest.writeByte((byte)(q instanceof String ? 0 : 1));
                    if (q instanceof String)
                        CBUtil.writeLongString((String)q, dest);
                    else
                        CBUtil.writeBytes(((MD5Digest)q).bytes, dest);

                    CBUtil.writeValueList(msg.values.get(i), dest);
                }

                CBUtil.writeConsistencyLevel(msg.consistency, dest);
            }

            @Override public int encodedSize(Batch msg) {
                int size = 3; // type + nb queries
                for (int i = 0; i < msg.queryOrIdList.size(); i++) {
                    Object q = msg.queryOrIdList.get(i);
                    size += 1 + (q instanceof String
                        ? CBUtil.sizeOfLongString((String)q)
                        : CBUtil.sizeOfBytes(((MD5Digest)q).bytes));

                    size += CBUtil.sizeOfValueList(msg.values.get(i));
                }
                size += CBUtil.sizeOfConsistencyLevel(msg.consistency);
                return size;
            }

            private byte fromType(BatchStatement.Type type) {
                switch (type) {
                    case LOGGED:   return 0;
                    case UNLOGGED: return 1;
                    case COUNTER:  return 2;
                    default:       throw new AssertionError();
                }
            }
        };

        public static final Message.Coder<Batch> coderV3 = new Message.Coder<Batch>() {
            @Override public void encode(Batch msg, ChannelBuffer dest) {
                int queries = msg.queryOrIdList.size();
                assert queries <= 0xFFFF;

                dest.writeByte(fromType(msg.type));
                dest.writeShort(queries);

                for (int i = 0; i < queries; i++) {
                    Object q = msg.queryOrIdList.get(i);
                    dest.writeByte((byte)(q instanceof String ? 0 : 1));
                    if (q instanceof String)
                        CBUtil.writeLongString((String)q, dest);
                    else
                        CBUtil.writeBytes(((MD5Digest)q).bytes, dest);

                    CBUtil.writeValueList(msg.values.get(i), dest);
                }

                CBUtil.writeConsistencyLevel(msg.consistency, dest);

                int flags = 0;
                if (msg.serialConsistency != null)  flags |= 0x10;
                if (msg.defaultTimestamp != 0L)     flags |= 0x20;
                dest.writeByte(flags);
                if (msg.serialConsistency != null)  CBUtil.writeConsistencyLevel(msg.serialConsistency, dest);
                if (msg.defaultTimestamp != 0L)     dest.writeLong(msg.defaultTimestamp);
            }

            @Override public int encodedSize(Batch msg) {
                int size = 3; // type + nb queries
                for (int i = 0; i < msg.queryOrIdList.size(); i++) {
                    Object q = msg.queryOrIdList.get(i);
                    size += 1 + (q instanceof String
                        ? CBUtil.sizeOfLongString((String)q)
                        : CBUtil.sizeOfBytes(((MD5Digest)q).bytes));

                    size += CBUtil.sizeOfValueList(msg.values.get(i));
                }
                size += CBUtil.sizeOfConsistencyLevel(msg.consistency);
                size += 1;
                if (msg.serialConsistency != null)  size += CBUtil.sizeOfConsistencyLevel(msg.consistency);
                if (msg.defaultTimestamp != 0L)     size += 4;
                return size;
            }

            private byte fromType(BatchStatement.Type type) {
                switch (type) {
                    case LOGGED:   return 0;
                    case UNLOGGED: return 1;
                    case COUNTER:  return 2;
                    default:       throw new AssertionError();
                }
            }
        };

        public final BatchStatement.Type type;
        public final List<Object> queryOrIdList;
        public final List<List<ByteBuffer>> values;
        public final ConsistencyLevel consistency;
        public final ConsistencyLevel serialConsistency;
        public final Long defaultTimestamp;

        public Batch(BatchStatement.Type type, List<Object> queryOrIdList, List<List<ByteBuffer>> values, ConsistencyLevel consistency,
                     ConsistencyLevel serialConsistency, long defaultTimestamp) {
            super(Message.Request.Type.BATCH);
            this.type = type;
            this.queryOrIdList = queryOrIdList;
            this.values = values;
            this.consistency = consistency;
            this.serialConsistency = serialConsistency;
            this.defaultTimestamp = defaultTimestamp;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("BATCH of [");
            for (int i = 0; i < queryOrIdList.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(queryOrIdList.get(i)).append(" with ").append(values.get(i).size()).append(" values");
            }
            sb.append("] at consistency ").append(consistency);
            if (serialConsistency != null)
                sb.append(", serialConsistency ").append(serialConsistency);
            if (defaultTimestamp != 0L)
                sb.append(", defaultTimestamp ").append(defaultTimestamp);
            return sb.toString();
        }
    }

    public static class Prepare extends Message.Request {

        public static final Message.Coder<Prepare> coder = new Message.Coder<Prepare>() {

            @Override public void encode(Prepare msg, ChannelBuffer dest) {
                CBUtil.writeLongString(msg.query, dest);
            }

            @Override public int encodedSize(Prepare msg) {
                return CBUtil.sizeOfLongString(msg.query);
            }
        };

        private final String query;

        public Prepare(String query) {
            super(Message.Request.Type.PREPARE);
            this.query = query;
        }

        @Override
        public String toString() {
            return "PREPARE " + query;
        }
    }

    public static class Register extends Message.Request {

        public static final Message.Coder<Register> coder = new Message.Coder<Register>() {
            @Override public void encode(Register msg, ChannelBuffer dest) {
                dest.writeShort(msg.eventTypes.size());
                for (ProtocolEvent.Type type : msg.eventTypes)
                    CBUtil.writeEnumValue(type, dest);
            }

            @Override public int encodedSize(Register msg) {
                int size = 2;
                for (ProtocolEvent.Type type : msg.eventTypes)
                    size += CBUtil.sizeOfEnumValue(type);
                return size;
            }
        };

        private final List<ProtocolEvent.Type> eventTypes;

        public Register(List<ProtocolEvent.Type> eventTypes) {
            super(Message.Request.Type.REGISTER);
            this.eventTypes = eventTypes;
        }

        @Override
        public String toString() {
            return "REGISTER " + eventTypes;
        }
    }

    public static class AuthResponse extends Message.Request {

        public static final Message.Coder<AuthResponse> coder = new Message.Coder<AuthResponse>() {

            @Override public void encode(AuthResponse response, ChannelBuffer dest) {
                CBUtil.writeValue(response.token, dest);
            }

            @Override public int encodedSize(AuthResponse response) {
                return CBUtil.sizeOfValue(response.token);
            }
        };

        private final byte[] token;

        public AuthResponse(byte[] token) {
            super(Message.Request.Type.AUTH_RESPONSE);
            this.token = token;
        }
    }
}
