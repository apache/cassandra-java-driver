/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.osgi;

import com.datastax.driver.core.CCMBridge;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

/**
 * A listener that fires up a single node CCM instance on test class start and tears it
 * down on test class end.
 * <p/>
 * This is needed for tests that use Pax-Exam since it runs some methods in the OSGi container
 * which we do not want.
 */
public class CCMBridgeListener implements ITestListener {

    private CCMBridge ccm;

    @Override
    public void onStart(ITestContext context) {
        ccm = CCMBridge.builder().withNodes(1).withBinaryPort(9042).build();
    }

    @Override
    public void onFinish(ITestContext context) {
        if (ccm != null) {
            ccm.remove();
        }
    }

    @Override
    public void onTestStart(ITestResult result) {
    }

    @Override
    public void onTestSuccess(ITestResult result) {
    }

    @Override
    public void onTestFailure(ITestResult result) {
    }

    @Override
    public void onTestSkipped(ITestResult result) {
    }

    @Override
    public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
    }
}
