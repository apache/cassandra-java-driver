# Contributing guidelines

## Working on an issue

Before starting to work on something, please comment in JIRA or ask on the mailing list
to make sure nobody else is working on it.

If your fix applies to multiple branches, base your work on the lowest active branch
(2.1 at the time of writing, but ask for confirmation). We regularly merge changes downstream.

Before you send your pull request, make sure that:

- you have a unit test that failed before the fix and succeeds after.
- the fix is mentioned in `changelog/README.md`.
- the commit message includes the reference of the JIRA ticket for automatic linking
  (example: `JAVA-503: Fix NPE when a connection fails during pool construction.`).

As long as your pull request is not merged, it's OK to rebase your branch and push with
`--force`.

If you want to contribute but don't have a specific issue in mind, the [lhf](https://datastax-oss.atlassian.net/secure/IssueNavigator.jspa?reset=true&mode=hide&jqlQuery=project%20%3D%20JAVA%20AND%20status%20in%20(Open%2C%20Reopened)%20AND%20labels%20%3D%20lhf)
label in JIRA is a good place to start: it marks "low hanging fruits" that don't require
in-depth knowledge of the codebase.

## Editor configuration

We use IntelliJ IDEA with the default formatting options, with one exception: check
"Enable formatter markers in comments" in Preferences > Editor > Code Style.

Please format your code and organize imports before submitting your changes.

## Running the tests

We use TestNG. There are 3 test categories:

- "unit": pure Java unit tests.
- "short" and "long": integration tests that launch Cassandra instances.

The Maven build uses profiles named after the categories to choose which tests to run:

```
mvn test -Pshort
```

The default is "unit". Each profile runs the ones before it ("short" runs unit, etc.)

Integration tests use [CCM](https://github.com/pcmanus/ccm) to bootstrap Cassandra instances.
Two Maven properties control its execution:

- `cassandra.version`: the Cassandra version. This has a default value in the root POM,
  you can override it on the command line (`-Dcassandra.version=...`).
- `ipprefix`: the prefix of the IP addresses that the Cassandra instances will bind to (see
  below). This defaults to `127.0.1.`.


CCM launches multiple Cassandra instances on localhost by binding to different addresses. The
driver uses up to 10 different instances (127.0.1.1 to 127.0.1.10 with the default prefix).
You'll need to define loopback aliases for this to work, on Mac OS X your can do it with:

```
sudo ifconfig lo0 alias 127.0.1.1 up
sudo ifconfig lo0 alias 127.0.1.2 up
...
```
