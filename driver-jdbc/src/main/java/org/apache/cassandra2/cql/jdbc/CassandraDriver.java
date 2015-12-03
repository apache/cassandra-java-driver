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
package org.apache.cassandra2.cql.jdbc;

import static com.datastax.driver.jdbc.Utils.NOT_SUPPORTED;
import static com.datastax.driver.jdbc.Utils.PROTOCOL;
import static com.datastax.driver.jdbc.Utils.TAG_PASSWORD;
import static com.datastax.driver.jdbc.Utils.TAG_USER;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;


import com.datastax.driver.jdbc.CassandraConnection;
import com.datastax.driver.jdbc.Utils;

/**
 * The Class CassandraDriver.
 */
public class CassandraDriver extends com.datastax.driver.jdbc.CassandraDriver implements Driver
{}
