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

import io.netty.buffer.ByteBuf;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

import static com.datastax.driver.core.SchemaElement.*;

class ProtocolEvent {

    enum Type {TOPOLOGY_CHANGE, STATUS_CHANGE, SCHEMA_CHANGE}

    final Type type;

    private ProtocolEvent(Type type) {
        this.type = type;
    }

    static ProtocolEvent deserialize(ByteBuf bb, ProtocolVersion version) {
        switch (CBUtil.readEnumValue(Type.class, bb)) {
            case TOPOLOGY_CHANGE:
                return TopologyChange.deserializeEvent(bb);
            case STATUS_CHANGE:
                return StatusChange.deserializeEvent(bb);
            case SCHEMA_CHANGE:
                return SchemaChange.deserializeEvent(bb, version);
        }
        throw new AssertionError();
    }

    static class TopologyChange extends ProtocolEvent {
        enum Change {NEW_NODE, REMOVED_NODE, MOVED_NODE}

        final Change change;
        final InetSocketAddress node;

        private TopologyChange(Change change, InetSocketAddress node) {
            super(Type.TOPOLOGY_CHANGE);
            this.change = change;
            this.node = node;
        }

        // Assumes the type has already been deserialized
        private static TopologyChange deserializeEvent(ByteBuf bb) {
            Change change = CBUtil.readEnumValue(Change.class, bb);
            InetSocketAddress node = CBUtil.readInet(bb);
            return new TopologyChange(change, node);
        }

        @Override
        public String toString() {
            return change + " " + node;
        }
    }

    static class StatusChange extends ProtocolEvent {

        enum Status {UP, DOWN}

        final Status status;
        final InetSocketAddress node;

        private StatusChange(Status status, InetSocketAddress node) {
            super(Type.STATUS_CHANGE);
            this.status = status;
            this.node = node;
        }

        // Assumes the type has already been deserialized
        private static StatusChange deserializeEvent(ByteBuf bb) {
            Status status = CBUtil.readEnumValue(Status.class, bb);
            InetSocketAddress node = CBUtil.readInet(bb);
            return new StatusChange(status, node);
        }

        @Override
        public String toString() {
            return status + " " + node;
        }
    }

    static class SchemaChange extends ProtocolEvent {

        enum Change {CREATED, UPDATED, DROPPED}

        final Change change;
        final SchemaElement targetType;
        final String targetKeyspace;
        final String targetName;
        final List<String> targetSignature;

        SchemaChange(Change change, SchemaElement targetType, String targetKeyspace, String targetName, List<String> targetSignature) {
            super(Type.SCHEMA_CHANGE);
            this.change = change;
            this.targetType = targetType;
            this.targetKeyspace = targetKeyspace;
            this.targetName = targetName;
            this.targetSignature = targetSignature;
        }

        // Assumes the type has already been deserialized
        static SchemaChange deserializeEvent(ByteBuf bb, ProtocolVersion version) {
            Change change;
            SchemaElement targetType;
            String targetKeyspace, targetName;
            List<String> targetSignature;
            switch (version) {
                case V1:
                case V2:
                    change = CBUtil.readEnumValue(Change.class, bb);
                    targetKeyspace = CBUtil.readString(bb);
                    targetName = CBUtil.readString(bb);
                    targetType = targetName.isEmpty() ? KEYSPACE : TABLE;
                    targetSignature = Collections.emptyList();
                    return new SchemaChange(change, targetType, targetKeyspace, targetName, targetSignature);
                case V3:
                case V4:
                case V5:
                    change = CBUtil.readEnumValue(Change.class, bb);
                    targetType = CBUtil.readEnumValue(SchemaElement.class, bb);
                    targetKeyspace = CBUtil.readString(bb);
                    targetName = (targetType == KEYSPACE) ? "" : CBUtil.readString(bb);
                    targetSignature = (targetType == FUNCTION || targetType == AGGREGATE)
                            ? CBUtil.readStringList(bb)
                            : Collections.<String>emptyList();
                    return new SchemaChange(change, targetType, targetKeyspace, targetName, targetSignature);
                default:
                    throw version.unsupported();
            }
        }

        @Override
        public String toString() {
            return change.toString() + ' ' + targetType + ' ' + targetKeyspace + (targetName.isEmpty() ? "" : '.' + targetName);
        }
    }

}
