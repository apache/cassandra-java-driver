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

import com.datastax.driver.core.UDTValue;


public class JdbcUdt extends AbstractJdbcType<UDTValue>
{    
    public static final JdbcUdt instance = new JdbcUdt();

    JdbcUdt() {}

    public boolean isCaseSensitive()
    {
        return true;
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

    public Class<UDTValue> getType()
    {
        return UDTValue.class;
    }

    public int getJdbcType()
    {
        return Types.OTHER;
    }

    public UDTValue compose(Object obj)
    {
        return (UDTValue)obj;
    }

    public Object decompose(UDTValue value)
    {
        return (Object) value;
    }

	@Override
	public int getScale(UDTValue obj) {
		// TODO Auto-generated method stub
		return -1;
	}

	@Override
	public int getPrecision(UDTValue obj) {
		// TODO Auto-generated method stub
		return -1;
	}

	@Override
	public String toString(UDTValue obj) {
		// TODO Auto-generated method stub
		return obj.toString();
	}

	
}
