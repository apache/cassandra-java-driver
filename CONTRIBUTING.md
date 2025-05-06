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

# Contributing to the Apache Cassandra Java Driver

Thank you for your interest in contributing!  

## Table of Contents
- [Ways to Contribute](#ways-to-contribute)
- [Contribution Process](#contribution-process)
  - [Reporting Issues](#reporting-issues)
  - [Submitting Changes (Pull Requests)](#submitting-changes-pull-requests)
- [Development Setup](#development-setup)
  - [Prerequisites](#prerequisites)
  - [Building the Project](#building-the-project)
  - [Running Tests](#running-tests)
- [Coding Guidelines](#coding-guidelines)
  - [General](#general)
  - [Code Formatting and License Headers](#code-formatting-and-license-headers)
  - [Javadoc](#javadoc)
  - [Logging](#logging)
  - [Don't abuse the stream API](#dont-abuse-the-stream-api)
  - [Never assume a specific format for toString()](#never-assume-a-specific-format-for-tostring)
  - [Concurrency annotations](#concurrency-annotations)
  - [Nullability annotations](#nullability-annotations)
- [Coding Guidelines for Tests](#coding-guidelines-for-tests)
  - [Coding style -- test code](#coding-style----test-code)
  - [Unit tests](#unit-tests)
  - [Integration tests](#integration-tests)

## Ways to Contribute

There are many ways to contribute, including:

- **Bug Reports**: Identify incorrect behavior, inconsistencies, or regressions in the driver. Provide reproduction steps when possible.
- **Feature Requests**: Propose improvements or new functionality. Please describe the use case (not just a proposed API).
- **Documentation Improvements**: Enhance guides, examples, javadocs, or configuration explanations.
- **Pull Requests**: Submit fixes, enhancements, performance improvements, or refactorings.
- **Testing Contributions**: Add missing tests, improve coverage, or enhance test infrastructure.
- **Support & Triage**: Help evaluate reported issues or contribute to discussions.
- **Verify Releases**: Verify the release artifacts work correctly in your environment, when a release is proposed in the mailing list.

### Communication
1. **Mailing Lists**: Mail to user-subscribe@cassandra.apache.org or dev-subscribe@cassandra.apache.org to join the user@cassandra.apache.org or dev@cassandra.apache.org mailing lists.
2. **Slack**: [#cassandra-drivers](https://the-asf.slack.com/archives/C05LPRVNZV1) channel in the Apache Software Foundation [Slack](https://infra.apache.org/slack.html). You can ask for an invite to the ASF Slack workspace in the mailing lists.
3. **JIRA**: https://issues.apache.org/jira/projects/CASSJAVA
4. **GitHub Repository**: https://github.com/apache/cassandra-java-driver


## Contribution Process

### Reporting Issues

All issues must be tracked in **Apache JIRA**:  
<https://issues.apache.org/jira/projects/CASSJAVA>

When filing an issue:

- Clearly describe the problem, expected behavior, and actual behavior.
- Include driver version, Java version, and Cassandra cluster details.
- Add reproduction steps or a minimal test case if possible.
- Use the appropriate issue type (Bug, Improvement, New Feature, etc.).
- Set the correct components (e.g., `core`, `mapper-runtime`, `quarkus`).

Committers will help refine the ticket if needed.

### Submitting Changes (Pull Requests)

All code changes require:

1. **A corresponding JIRA ticket** unless it's a ninja fix.  
   Include the JIRA key in the PR title, e.g.:  
   `CASSJAVA-40: Driver testing against Java 21`
   
2. **A pull request on GitHub**  
   Repository: <https://github.com/apache/cassandra-java-driver>

3. **Tests**  
   Every fix or feature should include or update tests. PRs without tests are rarely accepted.

4. **Documentation updates**  
   Update manual, javadocs, examples, or reference docs when applicable.

5. **Passing CI**  
   PRs must pass all CI jobs unless reviewers explicitly allow exceptions.

6. **Code review**  
   Committers will review your changes. 2 approvals from committers are required for merging.

7. **Squash**
   When the PR is ready to merge, use `git rebase -i` to squash your changes into a single commit before merging.
   The commit message should follow the format of
    ```txt
    <JIRA number> <JIRA title>
    patch by <Name>; reviewed by <Name> and <Name>
    ```
   For example,
    ```txt
    CASSJAVA-108 Update ESRI (and remove org.json) dependencies
    patch by Bret McGuire; reviewed by Bret McGuire and Lukasz Antoniak
    ```

**Do not** mix unrelated changes in one PR—keep contributions focused.

**Do not** base a PR on another one.

**Do not** squash commits before the PR is ready to merge.

---

## Development Setup

### Prerequisites

- **Java 8+** 
- **Maven 3.8.1+** 

### Building the Project

- Ensure Maven is installed and you are using Java 8.
- Build the project with:
  ```
  mvn clean package -DskipTests
  ```
- If using an IDE like IntelliJ and encountering issues with guava-shaded classes:
   - Run:
     ```
     mvn clean install -DskipTests
     ```
   - If IntelliJ uses a different Maven version, use the Maven window in IntelliJ: under `Lifecycle`, click `clean` and then `install`.

### Running Tests

#### Unit Tests

```bash
mvn clean install -DskipTests
mvn test
```

#### Integration Tests

1. Install Cassandra Cluster Manager (CCM) following its [README](https://github.com/apache/cassandra-ccm).
2.  **MacOS only**, for CCM and Simulacron-based tests, enable loopback aliases:
    ```shell
    for i in {2..255}; do sudo ifconfig lo0 alias 127.0.0.$i up; done
    ```
   Note: This may slow down networking. To remove the aliases after testing:
   ```shell
   for i in {2..255}; do sudo ifconfig lo0 -alias 127.0.0.$i up; done
   ```
3. **MacOS Apple Silicon only**, for some Cassandra versions, you might need to work around the JNA 5.6.0 version, which is incompatible to Apple Silicon.
    ```shell
    mvn dependency:get -Dartifact=net.java.dev.jna:jna:5.10.0
    cp ~/.m2/repository/net/java/dev/jna/jna/5.10.0/jna-5.10.0.jar ~/.ccm/repository/4.0.19/lib/jna-5.6.0.jar
    ```
4. Run integration tests:
   ```
   mvn clean verify
   ```
   To target a specific Cassandra version or distribution:
   ```
   mvn verify -Dccm.version=3.11.0
   mvn verify -Dccm.distribution=dse -Dccm.version=6.8.0
   ```

---

## Coding Guidelines

### General

Do not use static imports. They make things harder to understand when you look at the code 
someplace where you don't have IDE support, like Github's code view.

Avoid abbreviations in class and variable names. A good rule of thumb is that you should only use
them if you would also do so verbally, for example "id" and "config" are probably reasonable.
Single-letter variables are permissible if the variable scope is only a few lines, or for commonly
understood cases (like `i` for a loop index).

Keep source files short. Short files are easy to understand and test. The average should probably 
be around 200-300 lines. 

### Code Formatting and License Headers

- We follow the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html). See [google-java-format](https://github.com/google/google-java-format) for IDE plugins.
- To format code:
  ```
  # Java files
  mvn fmt:format
  # XML files
  mvn xml-format:xml-format
  # License headers
  mvn license:format
  ```

### Javadoc

All types in "API" packages must be documented. For "internal" packages, documentation is optional,
but in no way discouraged: it's generally a good idea to have a class-level comment that explains
where the component fits in the architecture, and anything else that you feel is important.

You don't need to document every parameter or return type, or even every method. Don't document 
something if it is completely obvious, we don't want to end up with this:


```java
/**
 * Returns the name.
 * 
 * @return the name
 */
String getName();
```

On the other hand, there is often something useful to say about a method, so most should have at
least a one-line comment. Use common sense.

Driver users coding in their IDE should find the right documentation at the right time. Try to 
think of how they will come into contact with the class. For example, if a type is constructed with
a builder, each builder method should probably explain what the default is when you don't call it.

Avoid using too many links, they can make comments harder to read, especially in the IDE. Link to a
type the first time it's mentioned, then use a text description ("this registry"...) or an `@code`
block. Don't link to a class in its own documentation. Don't link to types that appear right below
in the documented item's signature.

```java
/**
* @return this {@link Builder} <-- completely unnecessary
*/
Builder withLimit(int limit) {
```

### Logging

We use SLF4J; loggers are declared like this:

```java
private static final Logger LOG = LoggerFactory.getLogger(TheEnclosingClass.class);
```

Logs are intended for two personae:

* Ops who manage the application in production.
* Developers (maybe you) who debug a particular issue.

#### Log levels

* `ERROR`: something that renders the driver -- or a part of it -- completely unusable. An action is
  required to fix it: bouncing the client, applying a patch, etc.
* `WARN`: something that the driver can recover from automatically, but indicates a configuration or
  programming error that should be addressed. For example: the driver connected successfully, but 
  one of the contact points in the configuration was malformed; the same prepared statement is being
  prepared multiple time by the application code.
* `INFO`: something that is part of the normal operation of the driver, but might be useful to know
  for an operator. For example: the driver has initialized successfully and is ready to process
  queries; an optional dependency was detected in the classpath and activated an enhanced feature.

Do not log errors that are rethrown to the client (such as the error that you're going to complete a
request with). This is annoying for ops because they see a lot of stack traces that require no
actual action on their part, because they're already handled by application code.

Similarly, do not log stack traces for non-critical errors. If you still want the option to get the
trace for debugging, see the `Loggers.warnWithException` utility. 

The last 2 levels are for developers, to help follow what the driver is doing from a "black box"
perspective (think about debugging an issue remotely, and all you have are the logs).

* `TRACE`: anything that happens **for every user request**. Not only request handling, but all
  related components (e.g. timestamp generators, policies, etc).
* `DEBUG`: everything else. For example, node state changes, control connection activity, etc. 

Note that `DEBUG` and `TRACE` can coexist within the same component, for example the LBP
initializing is a one-time event, but returning a query plan is a per-request event.

**Log prefix** shows origin, e.g.:  
`[s0|90232530|0]` (session name | hash code of the CqlRequestHandler instance | number of request attempts)

Tests run with the configuration defined in `src/test/resources/logback-test.xml`. The default level
for driver classes is `WARN`, but you can override it with a system property: `-DdriverLevel=DEBUG`.
A nice setup is to use `DEBUG` when you run from your IDE, and keep the default for the command
line.

When you add or review new code, take a moment to run the tests in `DEBUG` mode and check if the
output looks good.

### Don't abuse the stream API

The `java.util.stream` API is often used (abused?) as a "functional API for collections":

```java
List<Integer> sizes = words.stream().map(String::length).collect(Collectors.toList());
```

The perceived advantages of this approach over traditional for-loops are debatable:

* readability: this is highly subjective. But consider the following:
  * everyone can read for-loops, whether they are familiar with the Stream API or not. The opposite
    is not true.
  * the stream API does not spell out all the details: what kind of list does `Collectors.toList()`
    return? Is it pre-sized? Mutable? Thread-safe?
  * the stream API looks pretty on simple examples, but things can get ugly fast. Try rewriting
    `NetworkTopologyReplicationStrategy` with streams.
* concision: this is irrelevant. When we look at code we care about maintainability, not how many
  keystrokes the author saved. The for-loop version of the above example is just 5 lines long, and
  your brain doesn't take longer to parse it.

The bottom line: don't try to "be functional" at all cost. Plain old for-loops are often just as
simple.

### Never assume a specific format for `toString()`

Only use `toString()` for debug logs or exception messages, and always assume that its format is
unspecified and can change at any time.
  
If you need a specific string representation for a class, make it a dedicated method with a
documented format, for example `toCqlLiteral`. Otherwise it's too easy to lose track of the intended
usage and break things: for example, someone modifies your `toString()` method to make their logs
prettier, but unintentionally breaks the script export feature that expected it to produce CQL
literals.
 
`toString()` can delegate to `toCqlLiteral()` if that is appropriate for logs. 


### Concurrency annotations

We use the [JCIP annotations](http://jcip.net/annotations/doc/index.html) to document thread-safety
policies.

Add them for all new code, with the exception of:

* enums and interfaces;
* utility classes (only static methods);
* test code.

Make sure you import the types from `net.jcip`, there are homonyms in the classpath.


### Nullability annotations

We use the [Spotbugs annotations](https://spotbugs.github.io) to document nullability of parameters,
method return types and class members.

Please annotate any new class or interface with the appropriate annotations: `@NonNull`, `@Nullable`. Make sure you import 
the types from `edu.umd.cs.findbugs.annotations`, there are homonyms in the classpath.


## Coding Guidelines for Tests

Static imports are permitted in a couple of places:
* All AssertJ methods, e.g.:
  ```java
  assertThat(node.getDatacenter()).isNotNull();
  fail("Expecting IllegalStateException to be thrown");
  ```
* All Mockito methods, e.g.:
  ```java
  when(codecRegistry.codecFor(DataTypes.INT)).thenReturn(codec);
  verify(codec).decodePrimitive(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
  ```
* All Awaitility methods, e.g.:
  ```java
  await().until(() -> somethingBecomesTrue());
  ```

Test methods names use lower snake case, generally start with `should`, and clearly indicate the
purpose of the test, for example: `should_fail_if_key_already_exists`. If you have trouble coming 
up with a simple name, it might be a sign that your test does too much, and should be split.

We use AssertJ (`assertThat`) for assertions. Don't use JUnit assertions (`assertEquals`, 
`assertNull`, etc).

Don't try to generify at all cost: a bit of duplication is acceptable, if that helps keep the tests
simple to understand (a newcomer should be able to understand how to fix a failing test without
having to read too much code).

Test classes can be a bit longer, since they often enumerate similar test cases. You can also
factor some common code in a parent abstract class named with "XxxTestBase", and then split
different families of tests into separate child classes. For example, `CqlRequestHandlerTestBase`,
`CqlRequestHandlerRetryTest`, `CqlRequestHandlerSpeculativeExecutionTest`...

### Unit tests

They live in the same module as the code they are testing. They should be fast and not start any
external process. They usually target one specific component and mock the rest of the driver
context.

### Integration tests

They live in the `integration-tests` module, and exercise the whole driver stack against an external
process, which can be either one of:
* [Simulacron](https://github.com/datastax/simulacron): simulates Cassandra nodes on loopback
  addresses; your test must "prime" data, i.e. tell the nodes what results to return for
  pre-determined queries.
  
    For an example of a Simulacron-based test, see `NodeTargetingIT`.
* [CCM](https://github.com/pcmanus/ccm): launches actual Cassandra nodes locally. The `ccm`
  executable must be in the path.
  
    You can pass a `-Dccm.version` system property to the build to target a particular Cassandra
    version (it defaults to 3.11.0). `-Dccm.directory` allows you to point to a local installation
    -- this can be a checkout of the Cassandra codebase, as long as it's built. See `CcmBridge` in
    the driver codebase for more details.
    
    For an example of a CCM-based test, see `PlainTextAuthProviderIT`.

#### Categories

Integration tests are divided into three categories:

##### Parallelizable tests

These tests can be run in parallel, to speed up the build. They either use:
* dedicated Simulacron instances. These are lightweight, and Simulacron will manage the ports to
  make sure that there are no collisions.
* a shared, one-node CCM cluster. Each test works in its own keyspace.

The build runs them with a configurable degree of parallelism (currently 8). The shared CCM cluster
is initialized the first time it's used, and stopped before moving on to serial tests. Note that we
run with `parallel=classes`, which means methods within the same class never run concurrent to each
other.

To make an integration test parallelizable, annotate it with `@Category(ParallelizableTests.class)`.
If you use CCM, it **must** be with `CcmRule`.

For an example of a Simulacron-based parallelizable test, see `NodeTargetingIT`. For a CCM-based
test, see `DirectCompressionIT`.

##### Serial tests

These tests cannot run in parallel, in general because they require CCM clusters of different sizes,
or with a specific configuration (we never run more than one CCM cluster simultaneously: it would be
too resource-intensive, and too complicated to manage all the ports).

The build runs them one by one, after the parallelizable tests.

To make an integration test serial, do not annotate it with `@Category`. The CCM rule **must** be
`CustomCcmRule`.

For an example, see `DefaultLoadBalancingPolicyIT`.
 
Note: if multiple serial tests have a common "base" class, do not pull up `CustomCcmRule`, each
child class must have its own instance. Otherwise they share the same CCM instance, and the first
one destroys it on teardown. See `TokenITBase` for how to organize code in those cases.

##### Isolated tests

Not only can those tests not run in parallel, they also require specific environment tweaks,
typically system properties that need to be set before initialization.

The build runs them one by one, *each in its own JVM fork*, after the serial tests.

To isolate an integration test, annotate it with `@Category(IsolatedTests.class)`. The CCM rule
**must** be `CustomCcmRule`.  

For an example, see `HeapCompressionIT`.

#### About test rules

Do not mix `CcmRule` and `SimulacronRule` in the same test. It makes things harder to follow, and
can be inefficient (if the `SimulacronRule` is method-level, it will create a Simulacron cluster for
every test method, even those that only need CCM).

Use the `@BackendRequirement` annotation to restrict the backend type and version. 
Specify the Cassandra Distribution, CASSANDRA/DSE/HCD, and the version requirement.
For example, `@BackendRequirement(type = BackendType.CASSANDRA, minInclusive = "2.2")`

##### Class-level rules

Rules annotated with `@ClassRule` wrap the whole test class, and are reused across methods. Try to
use this as much as possible, as it's more efficient. The fields need to be static; also make them
final and use constant naming conventions, like `CCM_RULE`.

When you use a server rule (`CcmRule` or `SimulacronRule`) and a `SessionRule` at the same level,
wrap them into a rule chain to ensure proper initialization order:

```java
private static final CcmRule CCM_RULE = CcmRule.getInstance();
private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

@ClassRule
public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);
```

##### Method-level rules

Rules annotated with `@Rule` wrap each test method. Use lower-camel case for field names:

```java
private CcmRule ccmRule = CcmRule.getInstance();
private SessionRule<CqlSession> sessionRule = SessionRule.builder(ccmRule).build();

@ClassRule
public TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);
```

Only use this for:

* CCM tests that use `@BackendRequirement` restrictions at the method level
  (ex: `BatchStatementIT`).
* tests where you *really* need to restart from a clean state for every method.

##### Mixed

It's also possible to use a `@ClassRule` for CCM / Simulacron, and a `@Rule` for the session rule.
In that case, you don't need to use a rule chain.
