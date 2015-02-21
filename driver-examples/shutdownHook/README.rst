ShutdownHook application
==================

A simple example application that uses the java driver and places the
cluster build and shutdown code close to each. This way things like
https://code.google.com/p/google-guice/wiki/ModulesShouldBeFastAndSideEffectFree
can be used in a simple way.

Usage
-----

You will need to build the shutdownhook application fist:
    
    ./bin/build

After which you can run it using for instance:
    
    ./bin/shutdownhook [running-cassandra-host-address]

Of course, you will need to have at least one Cassandra node running (on
127.0.0.1 by default) for this to work.
