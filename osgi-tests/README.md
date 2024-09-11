<!--
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
-->

# Java Driver OSGi Tests

This module contains OSGi tests for the driver.

It declares a typical "application" bundle containing a few services that rely 
on the driver, see `src/main`.

The integration tests in `src/tests` interrogate the application bundle services 
and check that they can operate normally. They exercise different provisioning
configurations to ensure that the driver is usable in most cases.

## Running the tests

In order to run the OSGi tests, all other driver modules must have been 
previously compiled, that is, their respective `target/classes` directory must 
be up-to-date and contain not only the class files, but also an up-to-date OSGi 
manifest.

Therefore, it is recommended to always compile all modules and run the OSGi
integration tests in one single pass, which can be easily done by running,
from the driver's parent module directory: 

    mvn clean verify
    
This will however also run other integration tests, and might take a long time
to finish. If you prefer to skip other integration tests, and only run the
OSGi ones, you can do so as follows:

    mvn clean verify \
        -DskipParallelizableITs=true \
        -DskipSerialITs=true \
        -DskipIsolatedITs=true

You can pass the following system properties to your tests:

1. `ccm.version=[version]`: the CCM version to use
2. `ccm.[dse|hcd]=true`: choose target backend type (e.g. DSE, HCD)
3. `osgi.debug=[true|false]`: whether to enable remote debugging of the OSGi container (see
   below).
   
## Debugging OSGi tests

First, you can enable DEBUG logs for the Pax Exam framework by editing the
`src/tests/resources/logback-test.xml` file.

Alternatively, you can debug the remote OSGi container by passing the system 
property `-Dosgi.debug=true`. In this case the framework will prompt for a
remote debugger on port 5005.
