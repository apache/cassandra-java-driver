/*
 *      Copyright (C) 2012-2015 DataStax Inc.
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

import java.util.Arrays;
import java.util.List;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class VersionNumberTest {

    @Test(groups = "unit")
    public void versionNumberTest() {

        String[] versions = new String[]{
                "1.2.0",
                "2.0.0",
                "2.0.0-beta1",
                "2.0.0-beta1-SNAPSHOT",
                "2.0.0-beta1-SNAPSHOT+abc01",
                "2.0.0.22" // DSE
        };

        VersionNumber[] numbers = new VersionNumber[versions.length];
        for (int i = 0; i < versions.length; i++)
            numbers[i] = VersionNumber.parse(versions[i]);

        for (int i = 0; i < versions.length; i++)
            assertEquals(numbers[i].toString(), versions[i]);

        assertEquals(numbers[0].compareTo(numbers[1]), -1);
        assertEquals(numbers[1].compareTo(numbers[2]), 1);
        assertEquals(numbers[2].compareTo(numbers[3]), -1);
        assertEquals(numbers[3].compareTo(numbers[4]), 0);
        assertEquals(numbers[1].compareTo(numbers[5]), -1);

        VersionNumber deb = VersionNumber.parse("2.0.0~beta1");
        assertEquals(deb, numbers[2]);

        VersionNumber shorter = VersionNumber.parse("2.0");
        assertEquals(shorter, numbers[1]);

        List<String> preReleaseLabels = Arrays.asList("beta1", "SNAPSHOT");
        assertEquals(numbers[3].getPreReleaseLabels(), preReleaseLabels);
        assertNull(numbers[0].getPreReleaseLabels());
    }
}
