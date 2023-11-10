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

## Common infrastructure

This covers utilities or concept that are shared throughout the codebase:

* the [context](context/) is what glues everything together, and your primary entry point to extend
  the driver.
* we explain the two major approaches to deal with [concurrency](concurrency/) in the driver.
* the [event bus](event_bus/) is used to decouple some of the internal components through
  asynchronous messaging.
