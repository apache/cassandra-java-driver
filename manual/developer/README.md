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

## Developer docs

This section explains how driver internals work. The intended audience is:

* driver developers and contributors;
* framework authors, or architects who want to write advanced customizations and integrations. 

Most of this material will involve "internal" packages; see [API conventions](../api_conventions/)
for more explanations.

We recommend reading about the [common infrastructure](common/) first. Then the documentation goes
from lowest to highest level:

* [Native protocol layer](native_protocol/): binary encoding of the TCP payloads;
* [Netty pipeline](netty_pipeline/): networking and low-level stream management;
* [Request execution](request_execution/): higher-level handling of user requests and responses;
* [Administrative tasks](admin/): everything else (cluster state and metadata).

If you're reading this on GitHub, the `.nav` file in each directory contains a suggested order.
