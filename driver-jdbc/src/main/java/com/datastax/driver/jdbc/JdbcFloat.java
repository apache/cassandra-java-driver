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
import java.sql.Types;



public class JdbcFloat extends AbstractJdbcType<Float>
{
    public static final JdbcFloat instance = new JdbcFloat();

    JdbcFloat() {}

    public boolean isCaseSensitive()
    {
        return false;
    }

    public int getScale(Float obj)
    {
        return 40;
    }

    public int getPrecision(Float obj)
    {
        return 7;
    }

    public boolean isCurrency()
    {
        return false;
    }

    public boolean isSigned()
    {
        return true;
    }

    public String toString(Float obj)
    {
        return obj.toString();
    }

    public boolean needsQuotes()
    {
        return false;
    }

    public Class<Float> getType()
    {
        return Float.class;
    }

    public int getJdbcType()
    {
        return Types.FLOAT;
    }

    public Float compose(Object value)
    {
        return (Float)value;
    }

    public Object decompose(Float value)
    {
        return (Object)value;
    }
}
