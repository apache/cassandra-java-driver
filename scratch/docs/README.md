0. [Testing Prerequisites](#testing-prerequisites)
1. [Building](#building)
2. [Testing](#testing)

Testing Prerequisites
=====================

Install CCM
-----------

    pip install ccm

Setup CCM Loopbacks
-------------------

    # For basic ccm
    sudo ifconfig lo0 alias 127.0.0.2 up
    sudo ifconfig lo0 alias 127.0.0.3 up

    # Additional loopbacks for java-driver testing
    sudo ifconfig lo0 alias 127.0.1.1 up
    sudo ifconfig lo0 alias 127.0.1.2 up
    sudo ifconfig lo0 alias 127.0.1.3 up
    sudo ifconfig lo0 alias 127.0.1.4 up


Building
========

    mvn clean package

Testing
=======

Unit Tests
----------

Use the following command to run only the unit tests:

    mvn test

_**Estimated Run Time**: x minutes_

Integration Tests
-----------------

The following command runs the full set of unit and integration tests:

    mvn verify

_**Estimated Run Time**: 4 minutes_

Coverage Report
---------------

The following command runs the full set of integration tests and produces a
coverage report:

    mvn cobertura:cobertura

Coverage report can be found at:

    driver-core/target/site/cobertura/index.html

_**Estimated Run Time**: 4 minutes_
