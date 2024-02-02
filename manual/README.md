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

## Manual

Driver modules:

* [Core](core/): the main entry point, deals with connectivity and query execution.
* [Query builder](query_builder/): a fluent API to create CQL queries programmatically.
* [Mapper](mapper/): generates the boilerplate to execute queries and convert the results into
  application-level objects.
* [Developer docs](developer/): explains the codebase and internal extension points for advanced
  customization.

Common topics:

* [API conventions](api_conventions/)
* [Case sensitivity](case_sensitivity/)
* [OSGi](osgi/)

```{eval-rst}
.. toctree::
   :hidden:
   :glob:
   
   api_conventions/*
   case_sensitivity/*
   core/*
   developer/*
   mapper/*
   osgi/*
   query_builder/*
```
