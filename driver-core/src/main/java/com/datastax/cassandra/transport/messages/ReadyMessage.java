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

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.datastax.cassandra.transport.Message;

/**
 * Message to indicate that the server is ready to receive requests.
 */
public class ReadyMessage extends Message.Response
{
    public static final Message.Codec<ReadyMessage> codec = new Message.Codec<ReadyMessage>()
    {
        public ReadyMessage decode(ChannelBuffer body)
        {
            return new ReadyMessage();
        }

        public ChannelBuffer encode(ReadyMessage msg)
        {
            return ChannelBuffers.EMPTY_BUFFER;
        }
    };

    public ReadyMessage()
    {
        super(Message.Type.READY);
    }

    public ChannelBuffer encode()
    {
        return codec.encode(this);
    }

    @Override
    public String toString()
    {
        return "READY";
    }
}
