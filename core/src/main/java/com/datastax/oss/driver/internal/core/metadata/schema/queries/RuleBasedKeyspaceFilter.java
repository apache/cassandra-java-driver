/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.metadata.schema.queries;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filters keyspaces during schema metadata queries.
 *
 * <p>Depending on the circumstances, we do it either on the server side with a WHERE IN clause that
 * will be appended to every query, or on the client side with a predicate that will be applied to
 * every fetched row.
 */
class RuleBasedKeyspaceFilter implements KeyspaceFilter {

  private static final Logger LOG = LoggerFactory.getLogger(RuleBasedKeyspaceFilter.class);

  private static final Pattern EXACT_INCLUDE = Pattern.compile("\\w+");
  private static final Pattern EXACT_EXCLUDE = Pattern.compile("!\\s*(\\w+)");
  private static final Pattern REGEX_INCLUDE = Pattern.compile("/(.+)/");
  private static final Pattern REGEX_EXCLUDE = Pattern.compile("!\\s*/(.+)/");

  private final String logPrefix;
  private final String whereClause;
  private final Set<String> exactIncludes = new HashSet<>();
  private final Set<String> exactExcludes = new HashSet<>();
  private final List<Predicate<String>> regexIncludes = new ArrayList<>();
  private final List<Predicate<String>> regexExcludes = new ArrayList<>();

  private final boolean isDebugEnabled;
  private final Set<String> loggedKeyspaces;

  RuleBasedKeyspaceFilter(@NonNull String logPrefix, @NonNull List<String> specs) {
    assert !specs.isEmpty(); // see KeyspaceFilter#newInstance

    this.logPrefix = logPrefix;
    for (String spec : specs) {
      spec = spec.trim();
      Matcher matcher;
      if (EXACT_INCLUDE.matcher(spec).matches()) {
        exactIncludes.add(spec);
        if (exactExcludes.remove(spec)) {
          LOG.warn(
              "[{}] '{}' is both included and excluded, ignoring the exclusion", logPrefix, spec);
        }
      } else if ((matcher = EXACT_EXCLUDE.matcher(spec)).matches()) {
        String name = matcher.group(1);
        if (exactIncludes.contains(name)) {
          LOG.warn(
              "[{}] '{}' is both included and excluded, ignoring the exclusion", logPrefix, name);
        } else {
          exactExcludes.add(name);
        }
      } else if ((matcher = REGEX_INCLUDE.matcher(spec)).matches()) {
        compile(matcher.group(1)).map(regexIncludes::add);
      } else if ((matcher = REGEX_EXCLUDE.matcher(spec)).matches()) {
        compile(matcher.group(1)).map(regexExcludes::add);
      } else {
        LOG.warn(
            "[{}] Error while parsing {}: invalid element '{}', skipping",
            logPrefix,
            DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES.getPath(),
            spec);
      }
    }

    if (!exactIncludes.isEmpty() && regexIncludes.isEmpty() && regexExcludes.isEmpty()) {
      // We can filter on the server
      whereClause = buildWhereClause(exactIncludes);
      if (!exactExcludes.isEmpty()) {
        // Proceed, but this is probably a mistake
        LOG.warn(
            "[{}] {} only has exact includes and excludes, the excludes are redundant",
            logPrefix,
            DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES.getPath());
      }
      LOG.debug("[{}] Filtering server-side with '{}'", logPrefix, whereClause);
    } else {
      whereClause = "";
      LOG.debug("[{}] No server-side filtering", logPrefix);
    }

    isDebugEnabled = LOG.isDebugEnabled();
    loggedKeyspaces = isDebugEnabled ? new HashSet<>() : null;
  }

  @NonNull
  @Override
  public String getWhereClause() {
    return whereClause;
  }

  @Override
  public boolean includes(@NonNull String keyspace) {
    if (exactIncludes.contains(keyspace)) {
      log(keyspace, true, "it is included by name");
      return true;
    } else if (exactExcludes.contains(keyspace)) {
      log(keyspace, false, "it is excluded by name");
      return false;
    } else if (regexIncludes.isEmpty()) {
      if (regexExcludes.isEmpty()) {
        log(keyspace, false, "it is not included by name");
        return false;
      } else if (matchesAny(keyspace, regexExcludes)) {
        log(keyspace, false, "it matches at least one regex exclude");
        return false;
      } else {
        log(keyspace, true, "it does not match any regex exclude");
        return true;
      }
    } else { // !regexIncludes.isEmpty()
      if (regexExcludes.isEmpty()) {
        if (matchesAny(keyspace, regexIncludes)) {
          log(keyspace, true, "it matches at least one regex include");
          return true;
        } else {
          log(keyspace, false, "it does not match any regex include");
          return false;
        }
      } else {
        if (matchesAny(keyspace, regexIncludes) && !matchesAny(keyspace, regexExcludes)) {
          log(keyspace, true, "it matches at least one regex include, and no regex exclude");
          return true;
        } else {
          log(keyspace, false, "it matches either no regex include, or at least one regex exclude");
          return false;
        }
      }
    }
  }

  private void log(@NonNull String keyspace, boolean include, @NonNull String reason) {
    if (isDebugEnabled && loggedKeyspaces.add(keyspace)) {
      LOG.debug(
          "[{}] Filtering {} '{}' because {}", logPrefix, include ? "in" : "out", keyspace, reason);
    }
  }

  private boolean matchesAny(String keyspace, List<Predicate<String>> rules) {
    for (Predicate<String> rule : rules) {
      if (rule.test(keyspace)) {
        return true;
      }
    }
    return false;
  }

  private Optional<Predicate<String>> compile(String regex) {
    try {
      return Optional.of(Pattern.compile(regex).asPredicate());
    } catch (PatternSyntaxException e) {
      LOG.warn(
          "[{}] Error while parsing {}: syntax error in regex /{}/ ({}), skipping",
          this.logPrefix,
          DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES.getPath(),
          regex,
          e.getMessage());
      return Optional.empty();
    }
  }

  private static String buildWhereClause(Set<String> keyspaces) {
    StringBuilder builder = new StringBuilder(" WHERE keyspace_name IN (");
    boolean first = true;
    for (String keyspace : keyspaces) {
      if (first) {
        first = false;
      } else {
        builder.append(",");
      }
      builder.append('\'').append(keyspace).append('\'');
    }
    return builder.append(')').toString();
  }
}
