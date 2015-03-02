package com.datastax.driver.jdbc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.DriverManager;
import java.sql.Statement;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;

import com.datastax.driver.core.CCMBridge;

public class BuildCluster {

	public static String HOST = System.getProperty("host", ConnectionDetails.getHost());
	public static CCMBridge ccmBridge = null;
	
	@BeforeSuite
    public static void setUpBeforeSuite() throws Exception
    {
    	/*System.setProperty("cassandra.version", "2.1.2");*/
    	ccmBridge = CCMBridge.create("jdbc_cluster");
    	ccmBridge.populate(2);
    	ccmBridge.start(1);
    	ccmBridge.waitForUp(1);
    	ccmBridge.start(2);
    	ccmBridge.waitForUp(2);
    	//cluster = ccmCluster.cluster;    	
    	//PORT = getPort();
    	HOST = CCMBridge.ipOfNode(1);        
   
	    }

	    
	@AfterSuite
	public static void tearDownAfterSuite() throws Exception
	{	    	
        System.out.println("Stopping nodes");
        ccmBridge.stop(1);
        ccmBridge.waitForDown(1);
        ccmBridge.stop(2);
        ccmBridge.waitForDown(2);
        System.out.println("Discarding cluster");
        ccmBridge.remove();
	}
	    
	/*public static void main(String[] args) throws IOException, InterruptedException {
		final Runtime runtime = Runtime.getRuntime();
		System.out.println("creation du cluster");		
		Process p = runtime.exec("cmd /C ccm create test2 --version 2.0.12 --nodes 1");
		int retValue = p.waitFor();

        BufferedReader outReaderOutput = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line = outReaderOutput.readLine();
        while (line != null) {
            System.out.println(line);
            line = outReaderOutput.readLine();
        }
		
		System.out.println("cluster créé");
		
		p = runtime.exec("cmd /C ccm node1 start --wait-for-binary-proto ");
		retValue = p.waitFor();

        outReaderOutput = new BufferedReader(new InputStreamReader(p.getInputStream()));
        line = outReaderOutput.readLine();
        while (line != null) {
            System.out.println(line);
            line = outReaderOutput.readLine();
        }
		System.out.println("Node1 lancé");
		p = runtime.exec("cmd /C ccm node1 stop");
		retValue = p.waitFor();

        outReaderOutput = new BufferedReader(new InputStreamReader(p.getInputStream()));
        line = outReaderOutput.readLine();
        while (line != null) {
            System.out.println(line);
            line = outReaderOutput.readLine();
        }
		System.out.println("Node1 stoppé");

	}*/

}
