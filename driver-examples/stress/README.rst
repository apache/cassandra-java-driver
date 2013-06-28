Stress application
==================

A simple example application that uses the java driver to stress test
Cassandra. This also somewhat stress test the java driver as a result.

Please note that this simple example is far from being a complete stress
application. In particular it currently supports a very limited number of
stress scenario.

Usage
-----

You will need to build the stress application fist:
    
    ./bin/build

After which you can run it using for instance:
    
    ./bin/stress insert_prepared

Of course, you will need to have at least one Cassandra node running (on
127.0.0.1 by default) for this to work. Please refer to:
    
    ./bin/stress -h

for more details on the options available.
