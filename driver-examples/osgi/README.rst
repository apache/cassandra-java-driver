..
   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at
   
     http://www.apache.org/licenses/LICENSE-2.0
   
   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.

OSGi Example
============

A simple example application that demonstrates using the Java Driver in
an OSGi service.

MailboxService is an activated service that uses Cassandra to
persist a Mailbox by email address.

Usage
-----

To build the bundle and run tests execute the following maven task::

    mvn integration-test -P short

The short profile needs to be activated since the tests run under
this group.

After which the bundle jar will be present in the target/ directory.

The project includes an integration test that verifies the service can
be activated and used in an OSGi container.  It also verifies that
driver-core can be used in an OSGi container in the following
configurations:

1. Default (default classifier with all dependencies)
2. Netty-Shaded (shaded classifier with all depedencies w/o Netty)
3. Guava 15 (default classifier with Guava 15.0)
4. Guava 16
5. Guava 17
6. Guava 18
