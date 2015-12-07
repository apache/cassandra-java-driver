# Contributing guidelines

## Working on an issue

Before starting to work on something, please comment in JIRA or ask on the mailing list
to make sure nobody else is working on it.

If a fix applies to 2.1 and 3.0, work on the 2.1 branch, your commit will eventually
get merged in 3.0. Other branches (2.0 and 2.2) are now discontinued.

Before you send your pull request, make sure that:

- you have a unit test that failed before the fix and succeeds after.
- the fix is mentioned in `changelog/README.md`.
- the commit message include the reference of the JIRA ticket for automatic linking
  (example: `JAVA-503: Fix NPE when a connection fails during pool construction.`).

As long as your pull request is not merged, it's OK to rebase your branch and push with
`--force`.

If you want to contribute but don't have a specific issue in mind, the [lhf](https://datastax-oss.atlassian.net/secure/IssueNavigator.jspa?reset=true&mode=hide&jqlQuery=project%20%3D%20JAVA%20AND%20status%20in%20(Open%2C%20Reopened)%20AND%20labels%20%3D%20lhf)
label in JIRA is a good place to start: it marks "low hanging fruits" that don't require
in-depth knowledge of the codebase.

## Editor configuration

### General

We consider automatic formatting as a help, not a crutch. Sometimes it makes sense to
break the rules to make the code more readable, for instance aligning columns (see the
constant declarations in `DataType.Name` for an example of this).

**Please do not reformat whole files, only the lines that you have added or modified**.


### Eclipse

Formatter:

- Preferences > Java > Code Style > Formatter.
- Click "Import".
- Select `src/main/config/ide/eclipse-formatter.xml`.

Import order:

- Preferences > Java > Code Style > Organize imports.
- Click "Import".
- Select `src/main/config/ide/eclipse.importorder`.

Prevent trailing whitespaces:

- Preferences > Java > Editor > Save Actions.
- Check "Perform the selected actions on save".
- Ensure "Format source code" and "Organize imports" are unchecked.
- Check "Additional actions".
- Click "Configure".
- In the "Code Organizing" tab, check "Remove trailing whitespace" and "All lines".
- Click "OK" (the text area should only have one action "Remove trailing white spaces").


### IntelliJ IDEA

- File > Import Settings...
- Select `src/main/config/ide/intellij-code-style.jar`.

This should add a new Code Style scheme called "java-driver".

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
driver uses up to 6 different instances (127.0.1.1 to 127.0.1.6 with the default prefix).
You'll need to define loopback aliases for this to work, on Mac OS X your can do it with:

```
sudo ifconfig lo0 alias 127.0.1.1 up
sudo ifconfig lo0 alias 127.0.1.2 up
...
```
