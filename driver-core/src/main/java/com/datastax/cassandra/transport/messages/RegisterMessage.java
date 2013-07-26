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
import java.util.List;

import com.datastax.cassandra.transport.Message;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.*;

public class RegisterMessage extends com.datastax.cassandra.transport.Message.Request
{
    public static final Message.Codec<RegisterMessage> codec = new Message.Codec<RegisterMessage>()
    {
        public RegisterMessage decode(ChannelBuffer body)
        {
            int length = body.readUnsignedShort();
            List<Event.Type> eventTypes = new ArrayList<Event.Type>(length);
            for (int i = 0; i < length; ++i)
                eventTypes.add(CBUtil.readEnumValue(Event.Type.class, body));
            return new RegisterMessage(eventTypes);
        }

        public ChannelBuffer encode(RegisterMessage msg)
        {
            ChannelBuffer cb = ChannelBuffers.dynamicBuffer();
            cb.writeShort(msg.eventTypes.size());
            for (Event.Type type : msg.eventTypes)
                cb.writeBytes(CBUtil.enumValueToCB(type));
            return cb;
        }
    };

    public final List<Event.Type> eventTypes;

    public RegisterMessage(List<Event.Type> eventTypes)
    {
        super(Message.Type.REGISTER);
        this.eventTypes = eventTypes;
    }

    public ChannelBuffer encode()
    {
        return codec.encode(this);
    }

    @Override
    public String toString()
    {
        return "REGISTER " + eventTypes;
    }
}
