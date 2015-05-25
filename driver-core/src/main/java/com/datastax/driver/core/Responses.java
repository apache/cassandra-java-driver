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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;

import com.datastax.driver.core.Responses.Result.Rows.Metadata;
import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.utils.Bytes;

import static com.datastax.driver.core.ProtocolVersion.V4;
import static com.datastax.driver.core.SchemaElement.KEYSPACE;
import static com.datastax.driver.core.SchemaElement.TABLE;

class Responses {

    private Responses() {}

    public static class Error extends Message.Response {

        public static final Message.Decoder<Error> decoder = new Message.Decoder<Error>() {
            @Override
            public Error decode(ByteBuf body, ProtocolVersion version) {
                ExceptionCode code = ExceptionCode.fromValue(body.readInt());
                String msg = CBUtil.readString(body);
                Object infos = null;
                switch (code) {
                    case UNAVAILABLE:
                        ConsistencyLevel clu = CBUtil.readConsistencyLevel(body);
                        int required = body.readInt();
                        int alive = body.readInt();
                        infos = new UnavailableException(clu, required, alive);
                        break;
                    case WRITE_TIMEOUT:
                    case READ_TIMEOUT:
                        ConsistencyLevel clt = CBUtil.readConsistencyLevel(body);
                        int received = body.readInt();
                        int blockFor = body.readInt();
                        if (code == ExceptionCode.WRITE_TIMEOUT) {
                            WriteType writeType = Enum.valueOf(WriteType.class, CBUtil.readString(body));
                            infos = new WriteTimeoutException(clt, writeType, received, blockFor);
                        } else {
                            byte dataPresent = body.readByte();
                            infos = new ReadTimeoutException(clt, received, blockFor, dataPresent != 0);
                        }
                        break;
                    case UNPREPARED:
                        infos = MD5Digest.wrap(CBUtil.readBytes(body));
                        break;
                    case ALREADY_EXISTS:
                        String ksName = CBUtil.readString(body);
                        String cfName = CBUtil.readString(body);
                        infos = new AlreadyExistsException(ksName, cfName);
                        break;
                }
                return new Error(version, code, msg, infos);
            }
        };

        public final ProtocolVersion serverProtocolVersion;
        public final ExceptionCode code;
        public final String message;
        public final Object infos; // can be null

        private Error(ProtocolVersion serverProtocolVersion, ExceptionCode code, String message, Object infos) {
            super(Message.Response.Type.ERROR);
            this.serverProtocolVersion = serverProtocolVersion;
            this.code = code;
            this.message = message;
            this.infos = infos;
        }

        public DriverException asException(InetSocketAddress host) {
            switch (code) {
                case SERVER_ERROR:     return new DriverInternalError(String.format("An unexpected error occurred server side on %s: %s", host, message));
                case PROTOCOL_ERROR:   return new DriverInternalError("An unexpected protocol error occurred. This is a bug in this library, please report: " + message);
                case BAD_CREDENTIALS:  return new AuthenticationException(host, message);
                case UNAVAILABLE:      return ((UnavailableException)infos).copy(); // We copy to have a nice stack trace
                case OVERLOADED:       return new OverloadedException(host, message);
                case IS_BOOTSTRAPPING: return new BootstrappingException(host, message);
                case TRUNCATE_ERROR:   return new TruncateException(message);
                case WRITE_TIMEOUT:    return ((WriteTimeoutException)infos).copy();
                case READ_TIMEOUT:     return ((ReadTimeoutException)infos).copy();
                case SYNTAX_ERROR:     return new SyntaxError(message);
                case UNAUTHORIZED:     return new UnauthorizedException(message);
                case INVALID:          return new InvalidQueryException(message);
                case CONFIG_ERROR:     return new InvalidConfigurationInQueryException(message);
                case ALREADY_EXISTS:   return ((AlreadyExistsException)infos).copy();
                case UNPREPARED:       return new UnpreparedException(host, message);
                default:               return new DriverInternalError(String.format("Unknown protocol error code %s returned by %s. The error message was: %s", code, host, message));
            }
        }

        @Override
        public String toString() {
            return "ERROR " + code + ": " + message;
        }
    }

    public static class Ready extends Message.Response {

        public static final Message.Decoder<Ready> decoder = new Message.Decoder<Ready>() {
            public Ready decode(ByteBuf body, ProtocolVersion version) {
                // TODO: Would it be cool to return a singleton? Check we don't need to
                // set the streamId or something
                return new Ready();
            }
        };

        public Ready() {
            super(Message.Response.Type.READY);
        }

        @Override
        public String toString() {
            return "READY";
        }
    }

    public static class Authenticate extends Message.Response {

        public static final Message.Decoder<Authenticate> decoder = new Message.Decoder<Authenticate>() {
            public Authenticate decode(ByteBuf body, ProtocolVersion version) {
                String authenticator = CBUtil.readString(body);
                return new Authenticate(authenticator);
            }
        };

        public final String authenticator;

        public Authenticate(String authenticator) {
            super(Message.Response.Type.AUTHENTICATE);
            this.authenticator = authenticator;
        }

        @Override
        public String toString() {
            return "AUTHENTICATE " + authenticator;
        }
    }

    public static class Supported extends Message.Response {

        public static final Message.Decoder<Supported> decoder = new Message.Decoder<Supported>() {
            public Supported decode(ByteBuf body, ProtocolVersion version) {
                return new Supported(CBUtil.readStringToStringListMap(body));
            }
        };

        public final Map<String, List<String>> supported;
        public final Set<ProtocolOptions.Compression> supportedCompressions = EnumSet.noneOf(ProtocolOptions.Compression.class);

        public Supported(Map<String, List<String>> supported) {
            super(Message.Response.Type.SUPPORTED);
            this.supported = supported;

            parseCompressions();
        }

        private void parseCompressions() {
            List<String> compList = supported.get(Requests.Startup.COMPRESSION_OPTION);
            if (compList == null)
                return;

            for (String compStr : compList) {
                ProtocolOptions.Compression compr = ProtocolOptions.Compression.fromString(compStr);
                if (compr != null)
                    supportedCompressions.add(compr);
            }
        }

        @Override
        public String toString() {
            return "SUPPORTED " + supported;
        }
    }

    public static abstract class Result extends Message.Response {

        public static final Message.Decoder<Result> decoder = new Message.Decoder<Result>() {
            public Result decode(ByteBuf body, ProtocolVersion version) {
                Kind kind = Kind.fromId(body.readInt());
                return kind.subDecoder.decode(body, version);
            }
        };

        public enum Kind {
            VOID         (1, Void.subcodec),
            ROWS         (2, Rows.subcodec),
            SET_KEYSPACE (3, SetKeyspace.subcodec),
            PREPARED     (4, Prepared.subcodec),
            SCHEMA_CHANGE(5, SchemaChange.subcodec);

            private final int id;
            final Message.Decoder<Result> subDecoder;

            private static final Kind[] ids;
            static {
                int maxId = -1;
                for (Kind k : Kind.values())
                    maxId = Math.max(maxId, k.id);
                ids = new Kind[maxId + 1];
                for (Kind k : Kind.values()) {
                    if (ids[k.id] != null)
                        throw new IllegalStateException("Duplicate kind id");
                    ids[k.id] = k;
                }
            }

            private Kind(int id, Message.Decoder<Result> subDecoder) {
                this.id = id;
                this.subDecoder = subDecoder;
            }

            public static Kind fromId(int id) {
                Kind k = ids[id];
                if (k == null)
                    throw new DriverInternalError(String.format("Unknown kind id %d in RESULT message", id));
                return k;
            }
        }

        public final Kind kind;

        protected Result(Kind kind) {
            super(Message.Response.Type.RESULT);
            this.kind = kind;
        }

        public static class Void extends Result {
            // Even though we have no specific information here, don't make a
            // singleton since as each message it has in fact a streamid and connection.
            public Void() {
                super(Kind.VOID);
            }

            public static final Message.Decoder<Result> subcodec = new Message.Decoder<Result>() {
                public Result decode(ByteBuf body, ProtocolVersion version) {
                    return new Void();
                }
            };

            @Override
            public String toString() {
                return "EMPTY RESULT";
            }
        }

        public static class SetKeyspace extends Result {
            public final String keyspace;

            private SetKeyspace(String keyspace) {
                super(Kind.SET_KEYSPACE);
                this.keyspace = keyspace;
            }

            public static final Message.Decoder<Result> subcodec = new Message.Decoder<Result>() {
                public Result decode(ByteBuf body, ProtocolVersion version) {
                    return new SetKeyspace(CBUtil.readString(body));
                }
            };

            @Override
            public String toString() {
                return "RESULT set keyspace " + keyspace;
            }
        }

        public static class Rows extends Result {

            public static class Metadata {

                private static enum Flag
                {
                    // The order of that enum matters!!
                    GLOBAL_TABLES_SPEC,
                    HAS_MORE_PAGES,
                    NO_METADATA;

                    public static EnumSet<Flag> deserialize(int flags) {
                        EnumSet<Flag> set = EnumSet.noneOf(Flag.class);
                        Flag[] values = Flag.values();
                        for (int n = 0; n < values.length; n++) {
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

                static final Metadata EMPTY = new Metadata(0, null, null, null);

                public final int columnCount;
                public final ColumnDefinitions columns; // Can be null if no metadata was asked by the query
                public final ByteBuffer pagingState;
                public final int[] pkIndices;

                private Metadata(int columnCount, ColumnDefinitions columns, ByteBuffer pagingState, int[] pkIndices) {
                    this.columnCount = columnCount;
                    this.columns = columns;
                    this.pagingState = pagingState;
                    this.pkIndices = pkIndices;
                }

                public static Metadata decode(ByteBuf body) {
                    return decode(body, false);
                }

                public static Metadata decode(ByteBuf body, boolean withPkIndices) {

                    // flags & column count
                    EnumSet<Flag> flags = Flag.deserialize(body.readInt());
                    int columnCount = body.readInt();

                    int[] pkIndices = null;
                    int pkCount;
                    if (withPkIndices && (pkCount = body.readInt()) > 0) {
                        pkIndices = new int[pkCount];
                        for (int i = 0; i < pkCount; i++)
                            pkIndices[i] = (int)body.readShort();
                    }

                    ByteBuffer state = null;
                    if (flags.contains(Flag.HAS_MORE_PAGES))
                        state = CBUtil.readValue(body);

                    if (flags.contains(Flag.NO_METADATA))
                        return new Metadata(columnCount, null, state, pkIndices);

                    boolean globalTablesSpec = flags.contains(Flag.GLOBAL_TABLES_SPEC);

                    String globalKsName = null;
                    String globalCfName = null;
                    if (globalTablesSpec) {
                        globalKsName = CBUtil.readString(body);
                        globalCfName = CBUtil.readString(body);
                    }

                    // metadata (names/types)
                    ColumnDefinitions.Definition[] defs = new ColumnDefinitions.Definition[columnCount];
                    for (int i = 0; i < columnCount; i++) {
                        String ksName = globalTablesSpec ? globalKsName : CBUtil.readString(body);
                        String cfName = globalTablesSpec ? globalCfName : CBUtil.readString(body);
                        String name = CBUtil.readString(body);
                        DataType type = DataType.decode(body);
                        defs[i] = new ColumnDefinitions.Definition(ksName, cfName, name, type);
                    }

                    return new Metadata(columnCount, new ColumnDefinitions(defs), state, pkIndices);
                }

                @Override
                public String toString() {
                    StringBuilder sb = new StringBuilder();

                    if (columns == null) {
                        sb.append('[').append(columnCount).append(" columns]");
                    } else {
                        for (ColumnDefinitions.Definition column : columns) {
                            sb.append('[').append(column.getName());
                            sb.append(" (").append(column.getType()).append(")]");
                        }
                    }
                    if (pagingState != null)
                        sb.append(" (to be continued)");
                    return sb.toString();
                }
            }

            public static final Message.Decoder<Result> subcodec = new Message.Decoder<Result>() {
                public Result decode(ByteBuf body, ProtocolVersion version) {

                    Metadata metadata = Metadata.decode(body);

                    int rowCount = body.readInt();
                    int columnCount = metadata.columnCount;

                    Queue<List<ByteBuffer>> data = new ArrayDeque<List<ByteBuffer>>(rowCount);
                    for (int i = 0; i < rowCount; i++) {
                        List<ByteBuffer> row = new ArrayList<ByteBuffer>(columnCount);
                        for (int j = 0; j < columnCount; j++)
                            row.add(CBUtil.readValue(body));
                        data.add(row);
                    }

                    return new Rows(metadata, data);
                }
            };

            public final Metadata metadata;
            public final Queue<List<ByteBuffer>> data;

            private Rows(Metadata metadata, Queue<List<ByteBuffer>> data) {
                super(Kind.ROWS);
                this.metadata = metadata;
                this.data = data;
            }

            @Override
            public String toString() {
                StringBuilder sb = new StringBuilder();
                sb.append("ROWS ").append(metadata).append('\n');
                for (List<ByteBuffer> row : data) {
                    for (int i = 0; i < row.size(); i++) {
                        ByteBuffer v = row.get(i);
                        if (v == null) {
                            sb.append(" | null");
                        } else {
                            sb.append(" | ");
                            if (metadata.columns == null) {
                                sb.append(Bytes.toHexString(v));
                            } else {
                                // We don't have the protocol version available and it's a pain to change
                                // everything to get it when this method is only ever call for debugging.
                                // So trying v3 and falling back to v2, which is ugly but good enough for now.
                                try {
                                    sb.append(metadata.columns.getType(i).deserialize(v, ProtocolVersion.V3));
                                } catch (IllegalArgumentException e) {
                                    sb.append(metadata.columns.getType(i).deserialize(v, ProtocolVersion.V2));
                                }
                            }
                        }
                    }
                    sb.append('\n');
                }
                sb.append("---");
                return sb.toString();
            }
        }

        public static class Prepared extends Result {

            public static final Message.Decoder<Result> subcodec = new Message.Decoder<Result>() {
                public Result decode(ByteBuf body, ProtocolVersion version) {
                    MD5Digest id = MD5Digest.wrap(CBUtil.readBytes(body));
                    boolean withPkIndices = version.compareTo(V4) >= 0;
                    Rows.Metadata metadata = Rows.Metadata.decode(body, withPkIndices);
                    Rows.Metadata resultMetadata = decodeResultMetadata(body, version);
                    return new Prepared(id, metadata, resultMetadata);
                }

                private Metadata decodeResultMetadata(ByteBuf body, ProtocolVersion version) {
                    switch (version) {
                        case V1:
                            return Rows.Metadata.EMPTY;
                        case V2:
                        case V3:
                        case V4:
                            return Rows.Metadata.decode(body);
                        default:
                            throw version.unsupported();
                    }
                }
            };

            public final MD5Digest statementId;
            public final Rows.Metadata metadata;
            public final Rows.Metadata resultMetadata;

            private Prepared(MD5Digest statementId, Rows.Metadata metadata, Rows.Metadata resultMetadata) {
                super(Kind.PREPARED);
                this.statementId = statementId;
                this.metadata = metadata;
                this.resultMetadata = resultMetadata;
            }

            @Override
            public String toString() {
                return "RESULT PREPARED " + statementId + ' ' + metadata + " (resultMetadata=" + resultMetadata + ')';
            }
        }

        public static class SchemaChange extends Result {

            public enum Change { CREATED, UPDATED, DROPPED }

            public final Change change;
            public final SchemaElement targetType;
            public final String targetKeyspace;
            public final String targetName;

            public static final Message.Decoder<Result> subcodec = new Message.Decoder<Result>() {
                public Result decode(ByteBuf body, ProtocolVersion version)
                {
                    // Note: the CREATE KEYSPACE/TABLE/TYPE SCHEMA_CHANGE response is different from the SCHEMA_CHANGE EVENT type
                    Change change;
                    SchemaElement target;
                    String keyspace, name;
                    switch (version) {
                        case V1:
                        case V2:
                            change = CBUtil.readEnumValue(Change.class, body);
                            keyspace = CBUtil.readString(body);
                            name = CBUtil.readString(body);
                            target = name.isEmpty() ? KEYSPACE : TABLE;
                            return new SchemaChange(change, target, keyspace, name);
                        case V3:
                        case V4:
                            change = CBUtil.readEnumValue(Change.class, body);
                            target = CBUtil.readEnumValue(SchemaElement.class, body);
                            keyspace = CBUtil.readString(body);
                            name = (target == KEYSPACE) ? "" : CBUtil.readString(body);
                            return new SchemaChange(change, target, keyspace, name);
                        default:
                            throw version.unsupported();
                    }
                }
            };

            private SchemaChange(Change change, SchemaElement targetType, String targetKeyspace, String targetName) {
                super(Kind.SCHEMA_CHANGE);
                this.change = change;
                this.targetType = targetType;
                this.targetKeyspace = targetKeyspace;
                this.targetName = targetName;
            }

            @Override
            public String toString() {
                return "RESULT schema change " + change + " on " + targetType + ' ' + targetKeyspace + (targetName.isEmpty() ? "" : '.' + targetName);
            }
        }
    }

    public static class Event extends Message.Response {

        public static final Message.Decoder<Event> decoder = new Message.Decoder<Event>() {
            public Event decode(ByteBuf body, ProtocolVersion version) {
                return new Event(ProtocolEvent.deserialize(body, version));
            }
        };

        public final ProtocolEvent event;

        public Event(ProtocolEvent event) {
            super(Message.Response.Type.EVENT);
            this.event = event;
        }

        @Override
        public String toString() {
            return "EVENT " + event;
        }
    }

    public static class AuthChallenge extends Message.Response {

        public static final Message.Decoder<AuthChallenge> decoder = new Message.Decoder<AuthChallenge>() {
            public AuthChallenge decode(ByteBuf body, ProtocolVersion version) {
                ByteBuffer b = CBUtil.readValue(body);
                if (b == null)
                    return new AuthChallenge(null);

                byte[] token = new byte[b.remaining()];
                b.get(token);
                return new AuthChallenge(token);
            }
        };

        public final byte[] token;

        private AuthChallenge(byte[] token) {
            super(Message.Response.Type.AUTH_CHALLENGE);
            this.token = token;
        }
    }

    public static class AuthSuccess extends Message.Response {

        public static final Message.Decoder<AuthSuccess> decoder = new Message.Decoder<AuthSuccess>() {
            public AuthSuccess decode(ByteBuf body, ProtocolVersion version) {
                ByteBuffer b = CBUtil.readValue(body);
                if (b == null)
                    return new AuthSuccess(null);

                byte[] token = new byte[b.remaining()];
                b.get(token);
                return new AuthSuccess(token);
            }
        };

        public final byte[] token;

        private AuthSuccess(byte[] token) {
            super(Message.Response.Type.AUTH_SUCCESS);
            this.token = token;
        }
    }
}
