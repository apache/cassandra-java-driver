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

# Contributing guidelines

## Code formatting

### Java

We follow the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html). See
https://github.com/google/google-java-format for IDE plugins. The rules are not configurable.

The build will fail if the code is not formatted. To format all files from the command line, run:
 
```
mvn fmt:format
```

Some aspects are not covered by the formatter: braces must be used with `if`, `else`, `for`, `do`
and `while` statements, even when the body is empty or contains only a single statement.

### XML

The build will fail if XML files are not formatted correctly. Run the following command before you
commit:

```java
mvn xml-format:xml-format
```

The formatter does not enforce a maximum line length, but please try to keep it below 100 characters
to keep files readable across all mediums (IDE, terminal, Github...).

### Other text files (markdown, etc)

Similarly, enforce a right margin of 100 characters in those files. Editors and IDEs generally have
a way to configure this (for IDEA, install the "Wrap to column" plugin).

## Coding style -- production code

Do not use static imports. They make things harder to understand when you look at the code 
someplace where you don't have IDE support, like Github's code view.

Avoid abbreviations in class and variable names. A good rule of thumb is that you should only use
them if you would also do so verbally, for example "id" and "config" are probably reasonable.
Single-letter variables are permissible if the variable scope is only a few lines, or for commonly
understood cases (like `i` for a loop index).

Keep source files short. Short files are easy to understand and test. The average should probably 
be around 200-300 lines. 

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

### Logs

We use SLF4J; loggers are declared like this:

```java
private static final Logger LOG = LoggerFactory.getLogger(TheEnclosingClass.class);
```

Logs are intended for two personae:

* Ops who manage the application in production.
* Developers (maybe you) who debug a particular issue.

The first 3 log levels are for ops:

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

Logs statements start with a prefix that identifies its origin, for example:

* for components that are unique to the cluster instance, just the cluster name: `[c0]`.
* for sessions, the cluster name + a generated unique identifier: `[c0|s0]`.
* for channel pools, the session identifier + the address of the node: `[c0|s0|/127.0.0.2:9042]`.
* for channels, the identifier of the owner (session or control connection) + the Netty identifier,
  which indicates the local and remote ports:
  `[c0|s0|id: 0xf9ef0b15, L:/127.0.0.1:51482 - R:/127.0.0.1:9042]`.
* for request handlers, the session identifier, a unique identifier, and the index of the 
  speculative execution: `[c0|s0|1077199500|0]`.

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


## Coding style -- test code

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

* CCM tests that use `@CassandraRequirement` or `@DseRequirement` restrictions at the method level
  (ex: `BatchStatementIT`).
* tests where you *really* need to restart from a clean state for every method.

##### Mixed

It's also possible to use a `@ClassRule` for CCM / Simulacron, and a `@Rule` for the session rule.
In that case, you don't need to use a rule chain.

## Running the tests

### Unit tests

    mvn clean test
    
This currently takes about 30 seconds. The goal is to keep it within a couple of minutes (it runs
for each commit if you enable the pre-commit hook -- see below).

### Integration tests

    mvn clean verify

This currently takes about 9 minutes. We don't have a hard limit, but ideally it should stay within
30 minutes to 1 hour.

You can skip test categories individually with `-DskipParallelizableITs`, `-DskipSerialITs` and
`-DskipIsolatedITs` (`-DskipITs` still works to skip them all at once).

### Configuring MacOS for Simulacron

Simulacron (used in integration tests) relies on loopback aliases to simulate multiple nodes. On
Linux or Windows, you shouldn't have anything to do. On MacOS, run this script:

```
#!/bin/bash
for sub in {0..4}; do
    echo "Opening for 127.0.$sub"
    for i in {0..255}; do sudo ifconfig lo0 alias 127.0.$sub.$i up; done
done
```

Note that this is known to cause temporary increased CPU usage in OS X initially while mDNSResponder
acclimates itself to the presence of added IP addresses. This lasts several minutes. Also, this does
not survive reboots.


## License headers

The build will fail if some license headers are missing. To update all files from the command line,
run:

```
mvn license:format
```

## Pre-commit hook (highly recommended)
 
Ensure `pre-commit.sh` is executable, then run:

```
ln -s ../../pre-commit.sh .git/hooks/pre-commit
```

This will only allow commits if the tests pass. It is also a good reminder to keep the test suite
short.

Note: the tests run on the current state of the working directory. I tried to add a `git stash` in
the script to only test what's actually being committed, but I couldn't get it to run reliably
(it's still in there but commented). Keep this in mind when you commit, and don't forget to re-add
the changes if the first attempt failed and you fixed the tests.

## Speeding up the build for local tests

If you need to install something in your local repository quickly, you can use the `fast` profile to
skip all "non-essential" checks (licenses, formatting, tests, etc):

```
mvn clean install -Pfast
```

You can speed things up even more by targeting specific modules with the `-pl` option:

```
mvn clean install -Pfast -pl core,query-builder,mapper-runtime,mapper-processor,bom
```

Please run the normal build at least once before you push your changes.

## Commits

Keep your changes **focused**. Each commit should have a single, clear purpose expressed in its 
message.

Resist the urge to "fix" cosmetic issues (add/remove blank lines, move methods, etc.) in existing
code. This adds cognitive load for reviewers, who have to figure out which changes are relevant to
the actual issue. If you see legitimate issues, like typos, address them in a separate commit (it's
fine to group multiple typo fixes in a single commit).

Isolate trivial refactorings into separate commits. For example, a method rename that affects dozens
of call sites can be reviewed in a few seconds, but if it's part of a larger diff it gets mixed up
with more complex changes (that might affect the same lines), and reviewers have to check every
line.

Commit message subjects start with a capital letter, use the imperative form and do **not** end
with a period:

* correct: "Add test for CQL request handler"
* incorrect: "~~Added test for CQL request handler~~"
* incorrect: "~~New test for CQL request handler~~"

Avoid catch-all messages like "Minor cleanup", "Various fixes", etc. They don't provide any useful
information to reviewers, and might be a sign that your commit contains unrelated changes.
 
We don't enforce a particular subject line length limit, but try to keep it short.

You can add more details after the subject line, separated by a blank line. The following pattern
(inspired by [Netty](http://netty.io/wiki/writing-a-commit-message.html)) is not mandatory, but
welcome for complex changes:

```
One line description of your change
 
Motivation:

Explain here the context, and why you're making that change.
What is the problem you're trying to solve.
 
Modifications:

Describe the modifications you've done.
 
Result:

After your change, what will change.
```

## Pull requests

Like commits, pull requests should be focused on a single, clearly stated goal.

Don't base a pull request onto another one, it's too complicated to follow two branches that evolve
at the same time. If a ticket depends on another, wait for the first one to be merged. 

If you have to address feedback, avoid rewriting the history (e.g. squashing or amending commits):
this makes the reviewers' job harder, because they have to re-read the full diff and figure out
where your new changes are. Instead, push a new commit on top of the existing history; it will be
squashed later when the PR gets merged. If the history is complex, it's a good idea to indicate in
the message where the changes should be squashed:

```
* 20c88f4 - Address feedback (to squash with "Add metadata parsing logic") (36 minutes ago)
* 7044739 - Fix various typos in Javadocs (2 days ago)
* 574dd08 - Add metadata parsing logic (2 days ago)
```

(Note that the message refers to the other commit's subject line, not the SHA-1. This way it's still
relevant if there are intermediary rebases.)

If you need new stuff from the base branch, it's fine to rebase and force-push, as long as you don't
rewrite the history. Just give a heads up to the reviewers beforehand. Don't push a merge commit to
a pull request.
