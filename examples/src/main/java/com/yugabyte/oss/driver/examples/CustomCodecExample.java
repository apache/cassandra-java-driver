package com.yugabyte.oss.driver.examples;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.yugabyte.oss.driver.internal.core.type.codec.extras.array.DoubleListToArrayCodec;
import com.yugabyte.oss.driver.internal.core.type.codec.extras.array.LongListToArrayCodec;

import java.net.InetSocketAddress;
import java.util.List;

public class CustomCodecExample {
    public static void main(String args[]){
        try {
            // Create a YCQL client.
            CqlSession session = CqlSession
                    .builder()
                    .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                    .withLocalDatacenter("datacenter1")
                    .addTypeCodecs(new DoubleListToArrayCodec(),
                            new LongListToArrayCodec())
                    .build();

            // Create keyspace 'ybdemo' if it does not exist.
            String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS ybdemo;";
            session.execute(createKeyspace);
            System.out.println("Created keyspace ybdemo");

            //Drop table
            String dropTable = "drop table if exists ybdemo.customcodectest;";
            session.execute(dropTable);
            System.out.println("Dropped table customcodectest");

            // Create table 'employee', if it does not exist.
            String createTable = "create table  ybdemo.customcodectest(id int Primary key, doublelist list<double>, longlist list<bigint>);";
            session.execute(createTable);
            System.out.println("Created table customcodectest");

            // Insert a row.
            String insert = "insert into ybdemo.customcodectest(id, doublelist, longlist) values (1, [1.0,2.0], [123456789, 987654321]);";
            session.execute(insert);
            System.out.println("Inserted data: " + insert);

            // Query the row and print out the result.
            String select = "SELECT * FROM ybdemo.customcodectest WHERE id = 1;";
            ResultSet selectResult = session.execute(select);
            List<Row> rows = selectResult.all();
            double[] doublelist = rows.get(0).get("doublelist",double[].class);
            long[] longlist = rows.get(0).get("longlist",long[].class);

            System.out.println("Double list returned:");
            for(int i = 0; i<doublelist.length;i++)
                System.out.print(doublelist[i] + ",");

            System.out.println("\nLong list returned:");
            for(int i = 0; i<longlist.length;i++)
                System.out.print(longlist[i] + ",");
            // Close the client.
            session.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
    }
