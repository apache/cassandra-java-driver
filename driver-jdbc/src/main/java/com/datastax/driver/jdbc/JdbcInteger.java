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
package com.datastax.driver.jdbc;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Types;



public class JdbcInteger extends AbstractJdbcType<BigInteger>
{
    public static final JdbcInteger instance = new JdbcInteger();

    JdbcInteger() {}

    public boolean isCaseSensitive()
    {
        return false;
    }

    public int getScale(BigInteger obj)
    {
        return 0;
    }

    public int getPrecision(BigInteger obj)
    {
        return obj.toString().length();
    }

    public boolean isCurrency()
    {
        return false;
    }

    public boolean isSigned()
    {
        return true;
    }

    public String toString(BigInteger obj)
    {
        return obj.toString();
    }

    public boolean needsQuotes()
    {
        return false;
    }

   

    public Class<BigInteger> getType()
    {
        return BigInteger.class;
    }

    public int getJdbcType()
    {
        return Types.BIGINT;
    }

    public BigInteger compose(Object obj)
    {
        return (BigInteger)obj;
    }

    public Object decompose(BigInteger value)
    {
        return (Object)value;
    }
}
