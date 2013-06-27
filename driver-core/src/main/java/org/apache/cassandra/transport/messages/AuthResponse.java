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
package org.apache.cassandra.transport.messages;

import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import java.nio.ByteBuffer;

/**
 * A SASL token message sent from client to server. Some SASL
 * mechanisms & clients may send an initial token before
 * receiving a challenge from the server.
 */
public class AuthResponse extends Message.Request
{
    public static final Message.Codec<AuthResponse> codec = new Message.Codec<AuthResponse>()
    {
        @Override
        public AuthResponse decode(ChannelBuffer body)
        {
            ByteBuffer b = CBUtil.readValue(body);
            byte[] token = new byte[b.remaining()];
            b.get(token);
            return new AuthResponse(token);
        }

        @Override
        public ChannelBuffer encode(AuthResponse response)
        {
            byte[] bytes = response.token;
            if (bytes == null || bytes.length == 0)
                return CBUtil.intToCB(0);

            return ChannelBuffers.wrappedBuffer(CBUtil.intToCB(bytes.length),
                                                ChannelBuffers.wrappedBuffer(bytes));
        }
    };

    private byte[] token;

    public AuthResponse(byte[] token)
    {
        super(Message.Type.AUTH_RESPONSE);
        this.token = token;
    }

    @Override
    public ChannelBuffer encode()
    {
        return codec.encode(this);
    }

    @Override
    public Response execute(QueryState queryState)
    {
        throw new UnsupportedOperationException("This class is provided in client code " +
                "for forward compatibility only so this should never be called on the client");
    }
}
