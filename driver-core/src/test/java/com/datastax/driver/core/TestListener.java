/*
 *      Copyright (C) 2012 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import org.testng.ITestResult;
import org.testng.TestListenerAdapter;

public class TestListener extends TestListenerAdapter {
	  private int m_count = 0;
	  
	  @Override
	  public void onTestFailure(ITestResult tr) {
	    log("F");
	  }
	 
	  @Override
	  public void onTestSkipped(ITestResult tr) {
	    log("S");
	  }
	 
	  @Override
	  public void onTestSuccess(ITestResult tr) {
	    log(".");
	  }
	 
	  private void log(String string) {
	    System.out.print(string);
	    if (++m_count % 40 == 0) {
	      System.out.println("");
	    }
	  }

}
