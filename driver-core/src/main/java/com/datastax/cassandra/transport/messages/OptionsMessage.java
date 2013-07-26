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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.FrameCompressor;
import com.datastax.cassandra.transport.Message;

/**
 * Message to indicate that the server is ready to receive requests.
 */
public class OptionsMessage extends Message.Request
{
    public static final Message.Codec<OptionsMessage> codec = new Message.Codec<OptionsMessage>()
    {
        public OptionsMessage decode(ChannelBuffer body)
        {
            return new OptionsMessage();
        }

        public ChannelBuffer encode(OptionsMessage msg)
        {
            return ChannelBuffers.EMPTY_BUFFER;
        }
    };

    public OptionsMessage()
    {
        super(Message.Type.OPTIONS);
    }

    public ChannelBuffer encode()
    {
        return codec.encode(this);
    }

    @Override
    public String toString()
    {
        return "OPTIONS";
    }
}
