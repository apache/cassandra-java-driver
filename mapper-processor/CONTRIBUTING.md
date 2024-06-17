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

# Mapper contributing guidelines

Everything in the [main contribution guidelines](../CONTRIBUTING.md) also applies to the mapper.
This file adds specific guidelines for the mapper.

## Testing

There are two ways to test the mapper:

### Unit tests

These tests reside here in the `mapper-processor` module. They run `javac` in-process with the
mapper's annotation processor configured. Each test creates its own input source code using the
JavaPoet DSL.

For an example, refer to any subclass of `MapperProcessorTest` (`DaoFactoryMethodGeneratorTest` is
one of them).

We don't fully validate the generated code, because it would be too much overhead to maintain those
checks anytime something changes. Therefore those tests should not try to cover semantic aspects of
the mapper, but instead focus on:

* errors and warnings, e.g. which method signatures are valid for a particular annotation, and what
  happens otherwise;
* presence and naming of the generated files for a particular input source.

### Integration tests

These tests reside in the usual `integration-tests` module. They cover the whole cycle: the module
contains annotated sources and runs the mapper processor as part of its build; the tests use the
generated code to interact with a CCM cluster.

For an example, see `GetEntityIT`.
