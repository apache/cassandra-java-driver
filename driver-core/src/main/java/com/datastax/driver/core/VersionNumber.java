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

import java.util.regex.Pattern;
import java.util.regex.Matcher;

import com.google.common.base.Objects;

/**
 * A version number in the form X.Y.Z with an optional pre-release label (like 1.2.14-SNAPSHOT).
 * <p>
 * Version numbers compare the usual way, the major number (X) is compared first, then the minor
 * one (Y) and then the patch level one (Z). Lastly, versions with pre-release sorts before the
 * versions that don't have one, and labels are sorted alphabetically if necessary.
 */
public class VersionNumber implements Comparable<VersionNumber> {

    private static final String VERSION_REGEXP = "(\\d+)\\.(\\d+)(\\.\\d+)?(\\-[.\\w]+)?";
    private static final Pattern pattern = Pattern.compile(VERSION_REGEXP);

    private final int major;
    private final int minor;
    private final int patch;
    private final String preRelease;

    private VersionNumber(int major, int minor, int patch, String preRelease) {
        this.major = major;
        this.minor = minor;
        this.patch = patch;
        this.preRelease = preRelease;
    }

    /**
     * Parse a version from a string (of the form X.Y.Z[-label]).
     * <p>
     * We also parse shorter versions like X.Y[-label] for convenience, but this parse
     * the same as X.Y.0[-label].
     *
     * @param version the string to parse
     * @throws IllegalArgumentException if the provided string does not
     * represent a valid version
     */
    public static VersionNumber parse(String version) {
        Matcher matcher = pattern.matcher(version);
        if (!matcher.matches())
            throw new IllegalArgumentException("Invalid version number: " + version);

        try {
            int major = Integer.parseInt(matcher.group(1));
            int minor = Integer.parseInt(matcher.group(2));

            String pa = matcher.group(3);
            int patch = pa == null || pa.isEmpty() ? 0 : Integer.parseInt(pa.substring(1)); // dropping the initial '.' since it's included this time

            String pr = matcher.group(4);
            String preRelease = pr == null || pr.isEmpty() ? null : pr.substring(1); // dropping the initial '-'

            return new VersionNumber(major, minor, patch, preRelease);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid version number: " + version);
        }
    }

    /**
     * The major version number.
     *
     * @return the major version number, i.e. X in X.Y.Z.
     */
    public int getMajor() {
        return major;
    }

    /**
     * The minor version number.
     *
     * @return the minor version number, i.e. Y in X.Y.Z.
     */
    public int getMinor() {
        return minor;
    }

    /**
     * The patch version number.
     *
     * @return the patch version number, i.e. Z in X.Y.Z.
     */
    public int getPatch() {
        return patch;
    }

    /**
     * The pre-release label if relevant, i.e. label in X.Y.Z-label.
     *
     * @return the pre-release label or {@code null} if the version number
     * doesn't have one.
     */
    public String getPreReleaseLabel() {
        return preRelease;
    }

    public int compareTo(VersionNumber other) {
        if (major < other.major)
            return -1;
        if (major > other.major)
            return 1;

        if (minor < other.minor)
            return -1;
        if (minor > other.minor)
            return 1;

        if (patch < other.patch)
            return -1;
        if (patch > other.patch)
            return 1;

        if (preRelease == null)
            return other.preRelease == null ? 0 : 1;

        return preRelease.compareTo(other.preRelease);
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof VersionNumber))
            return false;
        VersionNumber that = (VersionNumber)o;
        return major == that.major
            && minor == that.minor
            && patch == that.patch
            && Objects.equal(preRelease, that.preRelease);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(major, minor, patch, preRelease);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(major).append('.').append(minor).append('.').append(patch);
        if (preRelease != null)
            sb.append('-').append(preRelease);
        return sb.toString();
    }
}
