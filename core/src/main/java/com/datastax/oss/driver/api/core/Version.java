/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.driver.api.core;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A structured version number.
 *
 * <p>Versions are expected to follow the general guidelines of<a
 * href="https://semver.org/">semantic versioning</a>, but with a few relaxed rules.
 *
 * <p>The general form a version number is expected to be {@code X.Y[.Z]}, where X stands for the
 * version's major number, Y for the minor number, and Z for an (optional) patch number.
 *
 * <p>It can also contain a few optional extensions: a 4th "DSE patch" number (specific to DSE
 * releases); one or many pre-release labels; and one optional build label. See {@link
 * #parse(String)} for a few examples.
 *
 * <p>This class can be used to parse versions of Apache Cassandra®, DataStax Enterprise (DSE), or
 * the DataStax Java driver.
 *
 * <p>Version numbers compare the usual way, the major number (X) is compared first, then the minor
 * one (Y) and then the patch level one (Z). Lastly, versions with pre-release sorts before the
 * versions that don't have one, and labels are sorted alphabetically if necessary. Build metadata
 * are ignored when sorting versions.
 */
public class Version implements Comparable<Version> {

  private static final String VERSION_REGEXP =
      "(\\d+)\\.(\\d+)(\\.\\d+)?(\\.\\d+)?([~\\-]\\w[.\\w]*(?:-\\w[.\\w]*)*)?(\\+[.\\w]+)?";

  private static final Pattern pattern = Pattern.compile(VERSION_REGEXP);

  private final int major;
  private final int minor;
  private final int patch;
  private final int dsePatch;

  private final String[] preReleases;
  private final String build;

  private Version(
      int major, int minor, int patch, int dsePatch, String[] preReleases, String build) {
    this.major = major;
    this.minor = minor;
    this.patch = patch;
    this.dsePatch = dsePatch;
    this.preReleases = preReleases;
    this.build = build;
  }

  /**
   * Parses a version from a string.
   *
   * <p>The version string should have primarily the form {@code X.Y.Z}.
   *
   * <p>Out of convenience, the "patch" version number, {@code Z}, can be omitted, in which case it
   * is assumed to be 0.
   *
   * <p>After these first 3 elements, the following optional elements can be appended:
   *
   * <ol>
   *   <li>A 4th "DSE patch" number : {@code 5.0.2.7};
   *   <li>One or more pre-release labels after dashes or tildes: {@code 2.0.1-beta1}, {@code
   *       2.1.4~rc1-SNAPSHOT};
   *   <li>One build label starting with the plus sign: {@code 2.1.0-beta1+a20ba.sha}.
   * </ol>
   *
   * @param version the string to parse.
   * @return the parsed version.
   * @throws IllegalArgumentException if the provided string does not represent a valid version.
   */
  public static Version parse(String version) {
    if (version == null) {
      return null;
    }

    Matcher matcher = pattern.matcher(version);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid version number: " + version);
    }

    try {
      int major = Integer.parseInt(matcher.group(1));
      int minor = Integer.parseInt(matcher.group(2));

      String pa = matcher.group(3);
      int patch =
          pa == null || pa.isEmpty()
              ? 0
              : Integer.parseInt(
                  pa.substring(1)); // dropping the initial '.' since it's included this time

      String dse = matcher.group(4);
      int dsePatch =
          dse == null || dse.isEmpty()
              ? -1
              : Integer.parseInt(
                  dse.substring(1)); // dropping the initial '.' since it's included this time

      String pr = matcher.group(5);
      String[] preReleases =
          pr == null || pr.isEmpty()
              ? null
              : pr.substring(1)
                  .split("-"); // drop initial '-' or '~' then split on the remaining ones

      String bl = matcher.group(6);
      String build = bl == null || bl.isEmpty() ? null : bl.substring(1); // drop the initial '+'

      return new Version(major, minor, patch, dsePatch, preReleases, build);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid version number: " + version);
    }
  }

  /**
   * The major version number.
   *
   * @return the major version number, i.e. {@code X} in {@code X.Y.Z}.
   */
  public int getMajor() {
    return major;
  }

  /**
   * The minor version number.
   *
   * @return the minor version number, i.e. {@code Y} in {@code X.Y.Z}.
   */
  public int getMinor() {
    return minor;
  }

  /**
   * The patch version number.
   *
   * @return the patch version number, i.e. {@code Z} in {@code X.Y.Z}.
   */
  public int getPatch() {
    return patch;
  }

  /**
   * The DSE patch version number (will only be present for version of Cassandra in DSE).
   *
   * <p>DataStax Enterprise (DSE) adds a fourth number to the version number to track potential hot
   * fixes and/or DSE specific patches that may have been applied to the Cassandra version. In that
   * case, this method returns that fourth number.
   *
   * @return the DSE patch version number, i.e. {@code D} in {@code X.Y.Z.D}, or -1 if the version
   *     number is not from DSE.
   */
  public int getDSEPatch() {
    return dsePatch;
  }

  /**
   * The pre-release labels if relevant, i.e. {@code label1} and {@code label2} in {@code
   * X.Y.Z-label1-label2}.
   *
   * @return the pre-release labels. The returned list will be {@code null} if the version doesn't
   *     have any.
   */
  public List<String> getPreReleaseLabels() {
    return preReleases == null ? null : Collections.unmodifiableList(Arrays.asList(preReleases));
  }

  /**
   * The build label if there is one, i.e. {@code build123} in {@code X.Y.Z+build123}.
   *
   * @return the build label or {@code null} if the version number doesn't have one.
   */
  public String getBuildLabel() {
    return build;
  }

  /**
   * The next stable version, i.e. the version stripped of its pre-release labels and build
   * metadata.
   *
   * <p>This is mostly used during our development stage, where we test the driver against
   * pre-release versions of Cassandra like {@code 2.1.0-rc7-SNAPSHOT}, but need to compare to the
   * stable version {@code 2.1.0} when testing for native protocol compatibility, etc.
   *
   * @return the next stable version.
   */
  public Version nextStable() {
    return new Version(major, minor, patch, dsePatch, null, null);
  }

  @Override
  public int compareTo(Version other) {
    if (major < other.major) {
      return -1;
    }
    if (major > other.major) {
      return 1;
    }

    if (minor < other.minor) {
      return -1;
    }
    if (minor > other.minor) {
      return 1;
    }

    if (patch < other.patch) {
      return -1;
    }
    if (patch > other.patch) {
      return 1;
    }

    if (dsePatch < 0) {
      if (other.dsePatch >= 0) {
        return -1;
      }
    } else {
      if (other.dsePatch < 0) {
        return 1;
      }

      // Both are >= 0
      if (dsePatch < other.dsePatch) {
        return -1;
      }
      if (dsePatch > other.dsePatch) {
        return 1;
      }
    }

    if (preReleases == null) {
      return other.preReleases == null ? 0 : 1;
    }
    if (other.preReleases == null) {
      return -1;
    }

    for (int i = 0; i < Math.min(preReleases.length, other.preReleases.length); i++) {
      int cmp = preReleases[i].compareTo(other.preReleases[i]);
      if (cmp != 0) {
        return cmp;
      }
    }

    return Integer.compare(preReleases.length, other.preReleases.length);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof Version) {
      Version that = (Version) other;
      return this.major == that.major
          && this.minor == that.minor
          && this.patch == that.patch
          && this.dsePatch == that.dsePatch
          && (this.preReleases == null
              ? that.preReleases == null
              : Arrays.equals(this.preReleases, that.preReleases))
          && Objects.equals(this.build, that.build);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(major, minor, patch, dsePatch, Arrays.hashCode(preReleases), build);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(major).append('.').append(minor).append('.').append(patch);
    if (dsePatch >= 0) {
      sb.append('.').append(dsePatch);
    }
    if (preReleases != null) {
      for (String preRelease : preReleases) {
        sb.append('-').append(preRelease);
      }
    }
    if (build != null) {
      sb.append('+').append(build);
    }
    return sb.toString();
  }
}
