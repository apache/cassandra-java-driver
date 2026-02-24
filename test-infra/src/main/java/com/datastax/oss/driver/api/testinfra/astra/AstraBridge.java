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
package com.datastax.oss.driver.api.testinfra.astra;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AstraBridge extends CcmBridge {

  private static final Logger LOG = LoggerFactory.getLogger(AstraBridge.class);

  public static final BackendType DISTRIBUTION = BackendType.ASTRA;

  // Astra CLI configuration
  private static final String ASTRA_TOKEN = System.getProperty("astra.token");
  private static final String ASTRA_CLOUD_PROVIDER =
      System.getProperty("astra.cloud.provider", "gcp");
  private static final String ASTRA_REGION = System.getProperty("astra.region", "us-east1");

  // Existing database ID (if provided, use existing database instead of creating new one)
  private static final String ASTRA_DATABASE_ID = System.getProperty("astra.database.id");

  // Database configuration
  private static final String DATABASE_NAME_PREFIX = "java_driver_test_db_";
  private static final String DEFAULT_KEYSPACE = "java_driver_test";

  private final String databaseName;
  private final String keyspace;

  @SuppressWarnings("UnusedVariable")
  private final String cloudProvider;

  private final String region;
  private final AtomicBoolean created = new AtomicBoolean();
  private final AtomicBoolean started = new AtomicBoolean();
  private final Path configDirectory;

  // Flag to track if we're using an existing database (should not be destroyed)
  private final boolean usingExistingDatabase;

  private String databaseId;
  private File secureConnectBundle;

  private AstraBridge(
      Path configDirectory,
      String databaseName,
      String keyspace,
      String cloudProvider,
      String region,
      String existingDatabaseId) {
    super(
        configDirectory,
        new int[] {1},
        "127.0.0",
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList());
    this.configDirectory = configDirectory;
    this.keyspace = keyspace;
    this.cloudProvider = cloudProvider;
    this.region = region;
    this.usingExistingDatabase = existingDatabaseId != null;
    this.databaseId = existingDatabaseId; // Will be set if using existing database

    // If using existing database, extract the database name from Astra
    if (usingExistingDatabase) {
      try {
        // Setup Astra CLI with token first
        runAstraCommand(
            "config",
            "create",
            "java_driver_test",
            "--token",
            ASTRA_TOKEN,
            "--default",
            "--overwrite");

        // Get database info using CSV output format
        String dbInfoOutput = runAstraCommand("db", "get", existingDatabaseId, "-o", "csv");
        this.databaseName = extractDatabaseNameFromCsv(dbInfoOutput);
        LOG.info(
            "Extracted database name '{}' for existing database ID: {}",
            this.databaseName,
            existingDatabaseId);
      } catch (IOException | InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(
            "Failed to extract database name for existing database ID: " + existingDatabaseId, e);
      }
    } else {
      this.databaseName = databaseName;
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static boolean isDistributionOf(BackendType type) {
    return DISTRIBUTION == type;
  }

  public static Version getDistributionVersion() {
    // Astra uses Cassandra 4.0
    return Version.parse("4.0.0");
  }

  public static Version getCassandraVersion() {
    // Astra uses Cassandra 4.0
    return Version.parse("4.0.0");
  }

  public static class Builder extends CcmBridge.Builder {
    private String databaseName = DATABASE_NAME_PREFIX + System.currentTimeMillis();
    private String keyspace = DEFAULT_KEYSPACE;
    private String cloudProvider = ASTRA_CLOUD_PROVIDER;
    private String region = ASTRA_REGION;
    private String existingDatabaseId = ASTRA_DATABASE_ID;

    public Builder withDatabaseName(String databaseName) {
      this.databaseName = databaseName;
      return this;
    }

    public Builder withKeyspace(String keyspace) {
      this.keyspace = keyspace;
      return this;
    }

    public Builder withCloudProvider(String cloudProvider) {
      this.cloudProvider = cloudProvider;
      return this;
    }

    public Builder withRegion(String region) {
      this.region = region;
      return this;
    }

    public Builder withExistingDatabaseId(String databaseId) {
      this.existingDatabaseId = databaseId;
      return this;
    }

    @Override
    public AstraBridge build() {
      try {
        Path configDir = Files.createTempDirectory("astra-test-");
        return new AstraBridge(
            configDir, databaseName, keyspace, cloudProvider, region, existingDatabaseId);
      } catch (IOException e) {
        throw new RuntimeException("Failed to create config directory", e);
      }
    }
  }

  @Override
  public synchronized void create() {
    if (created.compareAndSet(false, true)) {
      try {
        // Setup Astra CLI with token (skip if already done in constructor for existing DB)
        if (!usingExistingDatabase) {
          runAstraCommand(
              "config",
              "create",
              "java_driver_test",
              "--token",
              ASTRA_TOKEN,
              "--default",
              "--overwrite");
        }

        if (usingExistingDatabase) {
          LOG.info("Using existing Astra database '{}' with ID: {}", databaseName, databaseId);

          // Download secure connect bundle for existing database
          downloadSecureConnectBundleById();

        } else {
          LOG.info("Creating Astra database: {}", databaseName);

          // Create database using Astra CLI
          // astra db create --no-async --non-vector --if-not-exists -k <KEYSPACE> -r <REGION>
          // <DB_NAME>
          List<String> createArgs = new ArrayList<>();
          createArgs.add("db");
          createArgs.add("create");
          createArgs.add("--no-async");
          createArgs.add("--non-vector");
          createArgs.add("--if-not-exists");
          createArgs.add("-k");
          createArgs.add(keyspace);
          createArgs.add("-r");
          createArgs.add(region);
          createArgs.add(databaseName);

          String output = runAstraCommand(createArgs.toArray(new String[0]));
          LOG.info("Database creation output: {}", output);

          // Get database ID using: astra db get <DB_NAME> --key id
          String dbIdOutput = runAstraCommand("db", "get", databaseName, "--key", "id");
          LOG.info("Database ID output: {}", dbIdOutput);

          // Extract the UUID from the output (filter out [INFO] and other lines)
          databaseId = extractDatabaseId(dbIdOutput);
          LOG.info("Astra database created with ID: {}", databaseId);

          // Download secure connect bundle
          downloadSecureConnectBundle();
        }

      } catch (IOException | InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Failed to create Astra database", e);
      }
    }
  }

  private void downloadSecureConnectBundle() throws IOException, InterruptedException {
    // Create SCB directory
    Path scbDir = configDirectory.resolve("scb");
    Files.createDirectories(scbDir);

    // Download SCB using: astra db download-scb <DB_NAME> -f <FILE>
    File scbFile = scbDir.resolve("scb_" + databaseId + "_" + region + ".zip").toFile();
    runAstraCommand("db", "download-scb", databaseName, "-f", scbFile.getAbsolutePath());

    if (!scbFile.exists()) {
      throw new IOException("Secure connect bundle was not downloaded: " + scbFile);
    }

    this.secureConnectBundle = scbFile;
    LOG.info("Secure connect bundle downloaded to: {}", scbFile.getAbsolutePath());
  }

  private void downloadSecureConnectBundleById() throws IOException, InterruptedException {
    // Create SCB directory
    Path scbDir = configDirectory.resolve("scb");
    Files.createDirectories(scbDir);

    // Download SCB using database ID: astra db download-scb <DB_ID> -f <FILE>
    File scbFile = scbDir.resolve("scb_" + databaseId + ".zip").toFile();
    runAstraCommand("db", "download-scb", databaseId, "-f", scbFile.getAbsolutePath());

    if (!scbFile.exists()) {
      throw new IOException("Secure connect bundle was not downloaded: " + scbFile);
    }

    this.secureConnectBundle = scbFile;
    LOG.info("Secure connect bundle downloaded by ID to: {}", scbFile.getAbsolutePath());
  }

  private static final Pattern UUID_PATTERN =
      Pattern.compile(
          "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

  /**
   * Extract database ID (UUID) from Astra CLI output. The output may contain [INFO] lines and other
   * messages, so we need to find the line that looks like a UUID. E.g. ✗ astra db get
   * java_driver_test_db_1765404086049 --key id [INFO] You are using a non-production environment
   * 'DEV' be2d3ad0-3bb0-4257-97d6-b83f2265b5f1
   */
  private String extractDatabaseId(String output) {
    // Use Pattern.compile to split by newline to avoid String.split() warning
    String[] lines = Pattern.compile("\n").split(output);
    for (String line : lines) {
      String trimmed = line.trim();
      // Skip lines that start with [INFO], [OK], [ERROR], etc.
      if (trimmed.startsWith("[")) {
        continue;
      }
      // Check if this line contains a UUID
      Matcher matcher = UUID_PATTERN.matcher(trimmed);
      if (matcher.find()) {
        return matcher.group();
      }
    }
    throw new IllegalStateException("Could not extract database ID from output: " + output);
  }

  /**
   * Extract database name from Astra CLI CSV output. The CSV output format is:
   *
   * <pre>
   * Attribute,Value
   * Name,java_driver_test_db_1767831564069
   * id,67750433-fe2e-48d4-bd60-7aef5e76be27
   * ...
   * </pre>
   *
   * @param output the CSV output from 'astra db get <DB_ID> -o csv'
   * @return the database name
   */
  private String extractDatabaseNameFromCsv(String output) {
    // Use Pattern.compile to split by newline to avoid String.split() warning
    String[] lines = Pattern.compile("\n").split(output);
    for (String line : lines) {
      String trimmed = line.trim();
      // Skip lines that start with [INFO], [OK], [ERROR], etc.
      if (trimmed.startsWith("[")) {
        continue;
      }
      // Skip the CSV header line
      if (trimmed.startsWith("code,message,attribute,value")) {
        continue;
      }
      // Look for the line with "Name" in the attribute column
      // Format: OK,,Name,<database_name> or Name,<database_name> (for older CLI versions)
      if (trimmed.contains(",Name,")) {
        // Split by comma and get the last part (the value)
        List<String> parts = Splitter.on(',').splitToList(trimmed);
        if (parts.size() >= 2) {
          // Return the last part which is the database name
          return parts.get(parts.size() - 1).trim();
        }
      } else if (trimmed.startsWith("Name,")) {
        // Handle older CSV format: Name,<database_name>
        String[] parts = trimmed.split(",", 2);
        if (parts.length == 2) {
          return parts[1].trim();
        }
      }
    }
    throw new IllegalStateException("Could not extract database name from CSV output: " + output);
  }

  /**
   * Checks if the error message indicates a transient error that should be retried.
   *
   * @param errorMessage the error message from the failed command
   * @return true if the error is retryable
   */
  private boolean isRetryableAstraError(String errorMessage) {
    if (errorMessage == null) {
      return false;
    }
    // Check for invalid state errors (e.g., MAINTENANCE mode)
    if (errorMessage.contains("invalid state")) {
      return true;
    }
    // Check for internal HTTP errors
    if (errorMessage.contains("INTERNAL_ERROR") && errorMessage.contains("HTTP Request")) {
      return true;
    }
    // Check for HttpEntity errors
    if (errorMessage.contains("HttpEntity")) {
      return true;
    }
    return false;
  }

  private String runAstraCommand(String... args) throws IOException, InterruptedException {
    int maxRetries = 3;
    int retryDelaySeconds = 10;
    IOException lastException = null;

    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        return executeAstraCommand(args);
      } catch (IOException e) {
        lastException = e;
        String errorMessage = e.getMessage();

        // Check if this is a retryable error
        if (isRetryableAstraError(errorMessage) && attempt < maxRetries) {
          LOG.error(
              "Astra CLI command failed with transient error (attempt {}/{}): {}",
              attempt + 1,
              maxRetries + 1,
              errorMessage);
          LOG.info("Waiting {} seconds before retry...", retryDelaySeconds);

          try {
            Thread.sleep(retryDelaySeconds * 1000L);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting to retry Astra CLI command", ie);
          }
          // Continue to next retry iteration
        } else {
          // Non-retryable error or max retries reached, throw immediately
          throw e;
        }
      }
    }

    // Should not reach here, but just in case
    throw lastException;
  }

  private String executeAstraCommand(String... args) throws IOException, InterruptedException {
    List<String> command = new ArrayList<>();
    command.add("astra");
    for (String arg : args) {
      command.add(arg);
    }

    ProcessBuilder pb = new ProcessBuilder(command);
    pb.redirectErrorStream(true);
    LOG.info("Running Astra CLI command: {} with environment: {}", String.join(" ", command), pb.environment().toString());
    Process process = pb.start();

    StringBuilder output = new StringBuilder();
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        output.append(line).append("\n");
        LOG.info("Astra CLI: {}", line);
      }
    }

    int exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new IOException(
          "Astra CLI command failed with exit code "
              + exitCode
              + ": "
              + String.join(" ", command)
              + "\nOutput: "
              + output);
    }

    return output.toString();
  }

  @Override
  public synchronized void start() {
    if (started.compareAndSet(false, true)) {
      create();
    }
  }

  @Override
  public synchronized void stop() {
    if (databaseId == null) {
      LOG.info("No Astra database to terminate");
      return;
    }

    if (usingExistingDatabase) {
      LOG.info("Using existing Astra database (ID: {}), skipping deletion", databaseId);
      return;
    }

    try {
      LOG.info("Terminating Astra database: {} (ID: {})", databaseName, databaseId);

      // Delete the database asynchronously (don't wait for completion)
      String deleteOutput = runAstraCommand("db", "delete", databaseName, "--async");
      LOG.info("Database deletion initiated: {}", deleteOutput);
      LOG.info("Astra database {} (ID: {}) is being terminated", databaseName, databaseId);
    } catch (Exception e) {
      LOG.warn("Failed to terminate Astra database {}: {}", databaseName, e.getMessage());
      LOG.info("To terminate manually, run: astra db delete {}", databaseName);
    }
  }

  @Override
  public void close() {
    stop();
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getDatabaseId() {
    return databaseId;
  }

  public String getKeyspace() {
    return keyspace;
  }

  public File getSecureConnectBundle() {
    return secureConnectBundle;
  }

  public String getToken() {
    return ASTRA_TOKEN;
  }

  @Override
  public BackendType getDistribution() {
    return DISTRIBUTION;
  }

  /**
   * Creates a new keyspace in the Astra database using the Astra CLI. Retries are handled
   * automatically by runAstraCommand() for transient errors.
   *
   * @param keyspaceName the name of the keyspace to create
   * @throws RuntimeException if the keyspace creation fails
   */
  public void createKeyspace(String keyspaceName) {
    if (databaseName == null) {
      throw new IllegalStateException(
          "Cannot create keyspace: Astra database has not been created yet");
    }

    try {
      LOG.info("Creating keyspace '{}' in Astra database '{}'", keyspaceName, databaseName);

      // Create keyspace using: astra db create-keyspace <DB_NAME> -k <KEYSPACE> --if-not-exists
      // runAstraCommand() will automatically retry on transient errors
      String output =
          runAstraCommand(
              "db",
              "create-keyspace",
              databaseName,
              "-k",
              keyspaceName,
              "--if-not-exists",
              "--timeout",
              "120");
      LOG.info("Keyspace creation output: {}", output);
      LOG.info("Keyspace '{}' created successfully", keyspaceName);

    } catch (IOException | InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(
          "Failed to create keyspace '" + keyspaceName + "' in database '" + databaseName + "'", e);
    }
  }

  /**
   * Drops all user-created tables in the specified keyspace. This is used to clean up after test
   * suites when running against Astra, where creating/dropping keyspaces is expensive.
   *
   * <p>System tables (those starting with "system") are not dropped.
   *
   * @param keyspaceName the name of the keyspace to clean
   * @param session the CQL session to use for dropping tables
   */
  public void dropAllTablesInKeyspace(String keyspaceName, CqlSession session) {
    if (databaseName == null) {
      throw new IllegalStateException(
          "Cannot drop tables: Astra database has not been created yet");
    }

    try {
      LOG.info(
          "Dropping all tables in keyspace '{}' of Astra database '{}'",
          keyspaceName,
          databaseName);

      // Get keyspace metadata from the driver
      Optional<KeyspaceMetadata> keyspaceMetadata = session.getMetadata().getKeyspace(keyspaceName);

      if (!keyspaceMetadata.isPresent()) {
        LOG.error("Keyspace '{}' not found in metadata", keyspaceName);
        return;
      }

      // Get all tables and UDTs from the keyspace metadata
      Map<CqlIdentifier, TableMetadata> tables = keyspaceMetadata.get().getTables();
      Map<CqlIdentifier, UserDefinedType> udts = keyspaceMetadata.get().getUserDefinedTypes();

      // IMPORTANT: Drop tables FIRST, then UDTs
      // UDTs cannot be dropped while tables are still using them

      // Step 1: Drop all tables
      AtomicInteger droppedTableCount = new AtomicInteger();
      for (TableMetadata tableMetadata : tables.values()) {
        CqlIdentifier tableId = tableMetadata.getName();
        String tableName = tableId.asInternal();
        if (!tableName.startsWith("system")) {
          try {
            // IMPORTANT: Use the CqlIdentifier to properly quote the table name
            // Tables created with quotes (e.g., "UPPER_CASE") need quotes to drop
            String dropStatement =
                String.format("DROP TABLE IF EXISTS %s.%s", keyspaceName, tableId.asCql(true));
            session.execute(
                SimpleStatement.newInstance(dropStatement)
                    .setTimeout(Duration.of(20, ChronoUnit.SECONDS)));
            droppedTableCount.getAndIncrement();
            LOG.info("Dropped table '{}.{}' using: {}", keyspaceName, tableName, dropStatement);
          } catch (DriverException e) {
            LOG.error("Failed to drop table '{}.{}': {}", keyspaceName, tableName, e.getMessage());
          }
        }
      }

      LOG.info("Dropped {} tables in keyspace '{}'", droppedTableCount.get(), keyspaceName);

      // Step 2: Drop all UDTs (after tables are dropped)
      AtomicInteger droppedUdtCount = new AtomicInteger();
      for (UserDefinedType udt : udts.values()) {
        CqlIdentifier udtId = udt.getName();
        String udtName = udtId.asInternal();
        try {
          // IMPORTANT: Use the CqlIdentifier to properly quote the UDT name
          String dropStatement =
              String.format("DROP TYPE IF EXISTS %s.%s", keyspaceName, udtId.asCql(true));
          session.execute(
              SimpleStatement.newInstance(dropStatement)
                  .setTimeout(Duration.of(20, ChronoUnit.SECONDS)));
          droppedUdtCount.getAndIncrement();
          LOG.info("Dropped type '{}.{}' using: {}", keyspaceName, udtName, dropStatement);
        } catch (DriverException e) {
          LOG.error("Failed to drop type '{}.{}': {}", keyspaceName, udtName, e.getMessage());
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to drop tables in keyspace '{}': {}", keyspaceName, e.getMessage(), e);
      // Don't throw - this is cleanup, we don't want to fail tests because of cleanup issues
    }
  }
}
