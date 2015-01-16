/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package org.apache.cassandra.cql.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;

abstract class AbstractStatement
{
    protected static final String NOT_SUPPORTED = "the Cassandra implementation does not support this method";

    /*
     * From the Statement Implementation
     */
    public void cancel() throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }
    
    public boolean execute(String arg0, int[] arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public boolean execute(String arg0, String[] arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public int executeUpdate(String arg0, int[] arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }
    
    public int executeUpdate(String arg0, String[] arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }
    public ResultSet getGeneratedKeys() throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }
   
    public void setCursorName(String arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void closeOnCompletion() throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public boolean isCloseOnCompletion() throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }
    
    /*
     * From the PreparedStatement Implementation
     */
    
    public void setArray(int parameterIndex, Array x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }


    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setClob(int parameterIndex, Clob x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

//    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException
//    {
//        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
//    }


//    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException
//    {
//        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
//    }



    public void setRef(int parameterIndex, Ref x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

}
