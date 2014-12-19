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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Types;


public class JdbcDecimal extends AbstractJdbcType<BigDecimal>
{
    public static final JdbcDecimal instance = new JdbcDecimal();

    JdbcDecimal() {}

    public boolean isCaseSensitive()
    {
        return false;
    }

    public int getScale(BigDecimal obj)
    {
        return obj.scale();
    }

    public int getPrecision(BigDecimal obj)
    {
        return obj.precision();
    }

    public boolean isCurrency()
    {
        return false;
    }

    public boolean isSigned()
    {
        return true;
    }

    public String toString(BigDecimal obj)
    {
        return obj.toPlainString();
    }

    public boolean needsQuotes()
    {
        return false;
    }

    public String getString(ByteBuffer bytes)
    {
        if (bytes == null) return "null";
        if (bytes.remaining() == 0) return "empty";
        return compose(bytes).toPlainString();
    }

    public Class<BigDecimal> getType()
    {
        return BigDecimal.class;
    }

    public int getJdbcType()
    {
        return Types.DECIMAL;
    }

    public BigDecimal compose(Object value)
    {        
        return (BigDecimal)value;
    }

    /**
     * The bytes of the ByteBuffer are made up of 4 bytes of int containing the scale
     * followed by the n bytes it takes to store a BigInteger.
     */
    public Object decompose(BigDecimal value)
    {        
        return (Object)value;
    }

}
