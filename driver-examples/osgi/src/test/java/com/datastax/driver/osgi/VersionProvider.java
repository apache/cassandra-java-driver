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
package com.datastax.driver.osgi;

import com.jcabi.manifests.Manifests;

/**
 * Attempts to resolve the project version from the Bundle manifest.  If not present, will throw RuntimeException
 * on initialization.   If this happens, try building with 'mvn compile' to generate the Bundle manifest.
 * <p/>
 * In IntelliJ you can have compile run after make by right clicking on 'compile' in the 'Maven Projects' tool window.
 */
public class VersionProvider {

    private static String PROJECT_VERSION;

    static {
        String bundleName = Manifests.read("Bundle-SymbolicName");
        if (bundleName.equals("com.datastax.driver.osgi")) {
            PROJECT_VERSION = Manifests.read("Bundle-Version").replaceAll("\\.SNAPSHOT", "-SNAPSHOT");
        } else {
            throw new RuntimeException("Couldn't resolve bundle manifest (try building with mvn compile)");
        }
    }

    public static String projectVersion() {
        return PROJECT_VERSION;
    }
}
