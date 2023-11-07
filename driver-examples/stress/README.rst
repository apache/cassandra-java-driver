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

Stress application
==================

A simple example application that uses the Java Driver to stress test
Cassandra. This also somewhat stress tests the Java Driver as a result.

Please note that this simple example is far from being a complete stress
application. In particular it currently supports a very limited number of
stress scenario.

Usage
-----

You will need to build the stress application fist:
    
    ./bin/build

After which you can run it using for instance:
    
    ./bin/stress insert_prepared

Of course, you will need to have at least one Cassandra node running (on
127.0.0.1 by default) for this to work. Please refer to:
    
    ./bin/stress -h

for more details on the options available.
