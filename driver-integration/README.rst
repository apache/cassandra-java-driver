Driver Integration Tests
========================

A set of modules representing possible packaging configurations that
the driver and tests to exercise these configurations.

This is not an exhaustive set of tests, it is merely to cover a set
of functionality that can not easily be exercised in the base test
cases since they require a very particular classpath configuration.

Usage
-----

To run all of these tests, execute the following:

    mvn integration-test -P short

Modules
-------

* integration-core - contains base test cases that are shared among
  other modules.
* integration-bare - a 'bare' configuration, meaning just the driver
  with no other optional dependencies such as compression libraries.
* integration-epoll - uses the netty-transport-native-epoll library
  to provide epoll for I/O.
* integration-shaded - uses the 'shaded' driver-core jar for using
  an embedded netty library.
