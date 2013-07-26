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

import java.util.UUID;

import com.datastax.cassandra.transport.Message;
import com.google.common.collect.ImmutableMap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.utils.UUIDGen;

/**
 * A CQL query
 */
public class QueryMessage extends com.datastax.cassandra.transport.Message.Request
{
    public static final Message.Codec<QueryMessage> codec = new Message.Codec<QueryMessage>()
    {
        public QueryMessage decode(ChannelBuffer body)
        {
            String query = CBUtil.readLongString(body);
            ConsistencyLevel consistency = CBUtil.readConsistencyLevel(body);
            return new QueryMessage(query, consistency);
        }

        public ChannelBuffer encode(QueryMessage msg)
        {

            return ChannelBuffers.wrappedBuffer(CBUtil.longStringToCB(msg.query), CBUtil.consistencyLevelToCB(msg.consistency));
        }
    };

    public final String query;
    public final ConsistencyLevel consistency;

    public QueryMessage(String query, ConsistencyLevel consistency)
    {
        super(Message.Type.QUERY);
        this.query = query;
        this.consistency = consistency;
    }

    public ChannelBuffer encode()
    {
        return codec.encode(this);
    }

    @Override
    public String toString()
    {
        return "QUERY " + query;
    }
}
