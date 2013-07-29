/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.cassandra.transport.messages;

import java.util.Map;

import com.datastax.cassandra.transport.Message;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.utils.SemanticVersion;

/**
 * The initial message of the protocol.
 * Sets up a number of connection options.
 */
public class StartupMessage extends com.datastax.cassandra.transport.Message.Request
{
    public static final String CQL_VERSION = "CQL_VERSION";
    public static final String COMPRESSION = "COMPRESSION";

    public static final Message.Codec<StartupMessage> codec = new Message.Codec<StartupMessage>()
    {
        public StartupMessage decode(ChannelBuffer body)
        {
            return new StartupMessage(CBUtil.readStringMap(body));
        }

        public ChannelBuffer encode(StartupMessage msg)
        {
            ChannelBuffer cb = ChannelBuffers.dynamicBuffer();
            CBUtil.writeStringMap(cb, msg.options);
            return cb;
        }
    };

    public final Map<String, String> options;

    public StartupMessage(Map<String, String> options)
    {
        super(Message.Type.STARTUP);
        this.options = options;
    }

    public ChannelBuffer encode()
    {
        return codec.encode(this);
    }

    public Message.Response execute(QueryState state)
    {
        // Several new message types have been backported from protocol v2
        // in order to support SASL authentication. To avoid conflicting versions
        // of the message classes on the classpath, we re-create the entire set
        // of messages in the com.datastax package. The execute method on Request
        // messages should only ever be called on the server side, so we throw
        // and exception if its called on the client.
        throw new IllegalStateException("This message should not be executed by the client");
    }

    @Override
    public String toString()
    {
        return "STARTUP " + options;
    }
}
