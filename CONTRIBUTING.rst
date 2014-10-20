Contributing guidelines
=======================

Working on an issue
-------------------

Before starting to work on something, please comment in JIRA or ask on the mailing list
to make sure nobody else is working on it.

If a fix applies both to 2.0 and 2.1, work on the 2.0 branch, your commit will eventually
get merged in 2.1.

Before you send your pull request, make sure that:

- you have a unit test that failed before the fix and succeeds after.
- the fix is mentioned in ``driver-core/CHANGELOG.rst``.
- the commit message include the reference of the JIRA ticket for automatic linking
  (example: ``Fix NPE when a connection fails during pool construction (JAVA-503).``).

As long as your pull request is not merged, it's OK to rebase your branch and push with
``--force``.

If you want to contribute but don't have a specific issue in mind, the `lhf <https://datastax-oss.atlassian.net/secure/IssueNavigator.jspa?reset=true&mode=hide&jqlQuery=project%20%3D%20JAVA%20AND%20status%20in%20(Open%2C%20Reopened)%20AND%20labels%20%3D%20lhf>`_
label in JIRA is a good place to start: it marks "low hanging fruits" that don't require
in-depth knowledge of the codebase.

Editor configuration
--------------------

General
~~~~~~~

We consider automatic formatting as a help, not a crutch. Sometimes it makes sense to
break the rules to make the code more readable, for instance aligning columns (see the
constant declarations in ``DataType.Name`` for an example of this).

**Please do not reformat whole files, only the lines that you have added or modified**.


Eclipse
~~~~~~~

Formatter:

- Preferences > Java > Code Style > Formatter.
- Click "Import".
- Select ``src/main/config/ide/eclipse-formatter.xml``.

Import order:

- Preferences > Java > Code Style > Organize imports.
- Click "Import".
- Select ``src/main/config/ide/eclipse.importorder``.

Prevent trailing whitespaces:

- Preferences > Java > Editor > Save Actions.
- Check "Perform the selected actions on save".
- Ensure "Format source code" and "Organize imports" are unchecked.
- Check "Additional actions".
- Click "Configure".
- In the "Code Organizing" tab, check "Remove trailing whitespace" and "All lines".
- Click "OK" (the text area should only have one action "Remove trailing white spaces").


IntelliJ IDEA
~~~~~~~~~~~~~

- File > Import Settings...
- Select ``src/main/config/ide/intellij-code-style.jar``.

This should add a new Code Style scheme called "java-driver".
