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

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.sql.Types;


public class JdbcAscii extends AbstractJdbcType<String>
{
    private static final Charset US_ASCII = Charset.forName("US-ASCII");
    public static final JdbcAscii instance = new JdbcAscii();

    JdbcAscii() {}

    public boolean isCaseSensitive()
    {
        return true;
    }

    public int getScale(String obj)
    {
        return -1;
    }

    public int getPrecision(String obj)
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

    public String toString(String obj)
    {
        return obj;
    }

    public boolean needsQuotes()
    {
        return true;
    }

    public String getString(Object obj)
    {
            return obj.toString();
        
    }

    public Class<String> getType()
    {
        return String.class;
    }

    public int getJdbcType()
    {
        return Types.VARCHAR;
    }

    public String compose(Object obj)
    {
        return obj.toString();
    }

    public Object decompose(String value)
    {
        return (Object) value;
    }

	
}
