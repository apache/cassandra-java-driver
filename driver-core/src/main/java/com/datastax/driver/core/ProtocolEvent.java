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

import java.net.InetSocketAddress;

import org.jboss.netty.buffer.ChannelBuffer;

class ProtocolEvent {

    public enum Type { TOPOLOGY_CHANGE, STATUS_CHANGE, SCHEMA_CHANGE }

    public final Type type;

    private ProtocolEvent(Type type) {
        this.type = type;
    }

    public static ProtocolEvent deserializeV1(ChannelBuffer cb) {
        switch (CBUtil.readEnumValue(Type.class, cb)) {
            case TOPOLOGY_CHANGE:
                return TopologyChange.deserializeEvent(cb);
            case STATUS_CHANGE:
                return StatusChange.deserializeEvent(cb);
            case SCHEMA_CHANGE:
                return SchemaChange.deserializeEventV1(cb);
        }
        throw new AssertionError();
    }

    public static ProtocolEvent deserializeV3(ChannelBuffer cb) {
        switch (CBUtil.readEnumValue(Type.class, cb)) {
            case TOPOLOGY_CHANGE:
                return TopologyChange.deserializeEvent(cb);
            case STATUS_CHANGE:
                return StatusChange.deserializeEvent(cb);
            case SCHEMA_CHANGE:
                return SchemaChange.deserializeEventV3(cb);
        }
        throw new AssertionError();
    }

    public static class TopologyChange extends ProtocolEvent {
        public enum Change { NEW_NODE, REMOVED_NODE, MOVED_NODE }

        public final Change change;
        public final InetSocketAddress node;

        private TopologyChange(Change change, InetSocketAddress node) {
            super(Type.TOPOLOGY_CHANGE);
            this.change = change;
            this.node = node;
        }

        // Assumes the type has already been deserialized
        private static TopologyChange deserializeEvent(ChannelBuffer cb) {
            Change change = CBUtil.readEnumValue(Change.class, cb);
            InetSocketAddress node = CBUtil.readInet(cb);
            return new TopologyChange(change, node);
        }

        @Override
        public String toString() {
            return change + " " + node;
        }
    }

    public static class StatusChange extends ProtocolEvent {

        public enum Status { UP, DOWN }

        public final Status status;
        public final InetSocketAddress node;

        private StatusChange(Status status, InetSocketAddress node) {
            super(Type.STATUS_CHANGE);
            this.status = status;
            this.node = node;
        }

        // Assumes the type has already been deserialized
        private static StatusChange deserializeEvent(ChannelBuffer cb) {
            Status status = CBUtil.readEnumValue(Status.class, cb);
            InetSocketAddress node = CBUtil.readInet(cb);
            return new StatusChange(status, node);
        }

        @Override
        public String toString() {
            return status + " " + node;
        }
    }

    public static class SchemaChange extends ProtocolEvent {

        public enum Change { CREATED, UPDATED, DROPPED }
        public enum Target { KEYSPACE, TABLE, TYPE }

        public final Change change;
        public final Target target;
        public final String keyspace;
        public final String name;

        public SchemaChange(Change change, Target target, String keyspace, String name) {
            super(Type.SCHEMA_CHANGE);
            this.change = change;
            this.target = target;
            this.keyspace = keyspace;
            this.name = name;
        }

        // Assumes the type has already been deserialized
        private static SchemaChange deserializeEventV1(ChannelBuffer cb) {
            Change change = CBUtil.readEnumValue(Change.class, cb);
            String keyspace = CBUtil.readString(cb);
            String name = CBUtil.readString(cb);
            Target target = name.isEmpty() ? Target.KEYSPACE : Target.TABLE;
            return new SchemaChange(change, target, keyspace, name);
        }

        // Assumes the type has already been deserialized
        private static SchemaChange deserializeEventV3(ChannelBuffer cb) {
            Change change = CBUtil.readEnumValue(Change.class, cb);
            Target target = CBUtil.readEnumValue(Target.class, cb);
            String keyspace = CBUtil.readString(cb);
            String name = "";
            switch (target) {
                case TABLE:
                case TYPE:
                    name = CBUtil.readString(cb);
                    break;
            }
            return new SchemaChange(change, target, keyspace, name);
        }

        @Override
        public String toString() {
            return change.toString() + ' ' + target + ' ' +keyspace + (name.isEmpty() ? "" : '.' + name);
        }
    }

}
