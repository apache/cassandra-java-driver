package com.datastax.driver.shutdownhook;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Session;

public class ShutdownHook {

    public static void main(String[] args)
    {
        String host = "127.0.0.1";
        if (args.length > 0 && args[0] != "-h") {
            host = args[0];
        }
        else {
            System.out.println("usage: shutdownhook [running-cassandra-host-address]");
            System.out.println("127.0.0.1 is used by default.");
        }

        System.out.println("Cassandra on " + host + " gets connected.");
        final Builder builder = Cluster.builder().addContactPoints(host);        
        final Cluster cluster = builder.build();
        // this hook stops the driver gracefully when the jvm has nothing more to do
        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            public void run()
            {
                cluster.close();
            }
        });
           
        Session session = cluster.connect();
        session.execute("SELECT COUNT(*) FROM system.hints LIMIT 1").one().getLong(0);
        System.out.println("A request successfully returned.");

        System.out.println("The Java main() method ends now.");
        System.out.println("The JVM should exit after the shutdown hook closed the cluster (stoped the driver gracefully).");    
    }

}
