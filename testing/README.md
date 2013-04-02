## Testing Prerequisites

### Install CCM

    pip install ccm

### Setup CCM Loopbacks (required for OSX)

    # For basic ccm
    sudo ifconfig lo0 alias 127.0.0.2 up
    sudo ifconfig lo0 alias 127.0.0.3 up

    # Additional loopbacks for java-driver testing
    sudo ifconfig lo0 alias 127.0.1.1 up
    sudo ifconfig lo0 alias 127.0.1.2 up
    sudo ifconfig lo0 alias 127.0.1.3 up
    sudo ifconfig lo0 alias 127.0.1.4 up
    sudo ifconfig lo0 alias 127.0.1.5 up
    sudo ifconfig lo0 alias 127.0.1.6 up



## Building the Driver

    mvn clean package



## Testing the Driver

### Unit Tests

Use the following command to run only the unit tests:

    mvn test

_**Estimated Run Time**: x minutes_

### Integration Tests

The following command runs the full set of unit and integration tests:

    mvn verify

_**Estimated Run Time**: 4 minutes_

### Coverage Report

The following command runs the full set of integration tests and produces a
coverage report:

    mvn cobertura:cobertura

Coverage report can be found at:

    driver-core/target/site/cobertura/index.html

_**Estimated Run Time**: 4 minutes_



## Test Utility

`testing/bin/coverage` exists to make testing a bit more straight-forward.

The main commands are as follows:

Displays the available parameters:

    testing/bin/coverage --help

Runs all the integration tests, creates the Cobertura report, and uploads Cobertura
site to a remote machine, if applicable:

    testing/bin/coverage

Runs a single integration test along with the Cobertura report for that test:

    testing/bin/coverage --test TestClass[#optionalTestMethod]
