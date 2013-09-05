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
package com.datastax.driver.core.sasl;

import com.datastax.driver.core.*;
import org.junit.Ignore;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Simple smoke test to verify the various auth provider implementions work as expected
 */
@Ignore
public class SmokeTest
{
    public static void main(String[] args)
    {
        AuthProvider authProvider = getKerberosAuthProvider();
//        AuthProvider authProvider = getStandardAuthProvider();
//        AuthProvider authProvider = getPlainTextSaslAuthProvider();
        Cluster cluster = Cluster.builder()
                                 .addContactPoint(args[0])
//                                 .withCredentials("cassandra", "cassandra")
                                 .withAuthProvider(authProvider)
                                 .build();
        Session session = cluster.connect();

        for (Row row : session.execute("SELECT * FROM system.local where key = 'local'"))
        {
            System.out.println(row.getString("cql_version"));
        }

        cluster.shutdown(5, TimeUnit.SECONDS);
        System.exit(0);
    }

    // An AuthProvider which uses SASL & GSSAPI to authenticate
    // needs a C* cluster with Kerberos auth enabled (& all
    // the supporting infrastructure - KDC etc)
    public static AuthProvider getKerberosAuthProvider()
    {
        return new DseAuthProvider();
    }

    // An AuthProvider which works for username/password
    // auth, but which uses the full SASL mechanism under the hood
    // just to verify that alternative SASL mechansisms are supported
    private static AuthProvider getPlainTextSaslAuthProvider()
    {
        return new PlainTextSaslAuthProvider("cassandra", "cassandra");
    }

    // Return an instance of the standard auth provider
    // (to handle username/password auth)
    public static AuthProvider getStandardAuthProvider()
    {
        return new PlainTextAuthProvider("cassandra", "cassandra");
    }
}
