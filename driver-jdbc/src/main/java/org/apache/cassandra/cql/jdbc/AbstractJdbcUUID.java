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
package org.apache.cassandra.cql.jdbc;

import java.sql.Types;
import java.util.UUID;

public abstract class AbstractJdbcUUID extends AbstractJdbcType<UUID>
{
    public String toString(UUID obj)
    {
        return obj.toString();
    }

    public boolean isCaseSensitive()
    {
        return false;
    }

    public int getScale(UUID obj)
    {
        return -1;
    }

    public int getPrecision(UUID obj)
    {
        return -1;
    }

    public boolean isCurrency()
    {
        return false;
    }

    public boolean isSigned()
    {
        return false;
    }

    public boolean needsQuotes()
    {
        return false;
    }

    public Class<UUID> getType()
    {
        return UUID.class;
    }

    public int getJdbcType()
    {
        return Types.OTHER;
    }
}
