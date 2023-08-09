#!groovy

def initializeEnvironment() {
  env.DRIVER_DISPLAY_NAME = 'CassandraⓇ Java Driver'
  env.DRIVER_METRIC_TYPE = 'oss'
  if (env.GIT_URL.contains('riptano/java-driver')) {
    env.DRIVER_DISPLAY_NAME = 'private ' + env.DRIVER_DISPLAY_NAME
    env.DRIVER_METRIC_TYPE = 'oss-private'
  } else if (env.GIT_URL.contains('java-dse-driver')) {
    env.DRIVER_DISPLAY_NAME = 'DSE Java Driver'
    env.DRIVER_METRIC_TYPE = 'dse'
  }

  env.GIT_SHA = "${env.GIT_COMMIT.take(7)}"
  env.GITHUB_PROJECT_URL = "https://${GIT_URL.replaceFirst(/(git@|http:\/\/|https:\/\/)/, '').replace(':', '/').replace('.git', '')}"
  env.GITHUB_BRANCH_URL = "${GITHUB_PROJECT_URL}/tree/${env.BRANCH_NAME}"
  env.GITHUB_COMMIT_URL = "${GITHUB_PROJECT_URL}/commit/${env.GIT_COMMIT}"

  env.MAVEN_HOME = "${env.HOME}/.mvn/apache-maven-3.3.9"
  env.PATH = "${env.MAVEN_HOME}/bin:${env.PATH}"
  env.JAVA_HOME = sh(label: 'Get JAVA_HOME',script: '''#!/bin/bash -le
    . ${JABBA_SHELL}
    jabba which ${JABBA_VERSION}''', returnStdout: true).trim()
  env.JAVA8_HOME = sh(label: 'Get JAVA8_HOME',script: '''#!/bin/bash -le
    . ${JABBA_SHELL}
    jabba which 1.8''', returnStdout: true).trim()

  sh label: 'Download Apache CassandraⓇ or DataStax Enterprise',script: '''#!/bin/bash -le
    . ${JABBA_SHELL}
    jabba use ${JABBA_VERSION}
    . ${CCM_ENVIRONMENT_SHELL} ${SERVER_VERSION}
  '''

  sh label: 'Display Java and environment information',script: '''#!/bin/bash -le
    # Load CCM environment variables
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    . ${JABBA_SHELL}
    jabba use ${JABBA_VERSION}

    java -version
    mvn -v
    printenv | sort
  '''
}

def buildDriver(jabbaVersion) {
  withEnv(["BUILD_JABBA_VERSION=${jabbaVersion}"]) {
    sh label: 'Build driver', script: '''#!/bin/bash -le
      . ${JABBA_SHELL}
      jabba use ${BUILD_JABBA_VERSION}

      mvn -B -V install -DskipTests -Dmaven.javadoc.skip=true
    '''
  }
}

def executeTests() {
  sh label: 'Execute tests', script: '''#!/bin/bash -le
    # Load CCM environment variables
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    . ${JABBA_SHELL}
    jabba use ${JABBA_VERSION}

    if [ "${JABBA_VERSION}" != "1.8" ]; then
      SKIP_JAVADOCS=true
    else
      SKIP_JAVADOCS=false
    fi

    INTEGRATION_TESTS_FILTER_ARGUMENT=""
    if [ ! -z "${INTEGRATION_TESTS_FILTER}" ]; then
      INTEGRATION_TESTS_FILTER_ARGUMENT="-Dit.test=${INTEGRATION_TESTS_FILTER}"
    fi
    printenv | sort

    mvn -B -V ${INTEGRATION_TESTS_FILTER_ARGUMENT} verify \
      -DfailIfNoTests=false \
      -Dmaven.test.failure.ignore=true \
      -Dmaven.javadoc.skip=${SKIP_JAVADOCS} \
      -Dccm.version=${CCM_CASSANDRA_VERSION} \
      -Dccm.dse=${CCM_IS_DSE} \
      -Dproxy.path=${HOME}/proxy \
      ${SERIAL_ITS_ARGUMENT} \
      ${ISOLATED_ITS_ARGUMENT} \
      ${PARALLELIZABLE_ITS_ARGUMENT}
  '''
}

def executeCodeCoverage() {
  jacoco(
    execPattern: '**/target/jacoco.exec',
    classPattern: '**/classes',
    sourcePattern: '**/src/main/java'
  )
}

def notifySlack(status = 'started') {
  // Notify Slack channel for every build except adhoc executions
  if (params.ADHOC_BUILD_TYPE != 'BUILD-AND-EXECUTE-TESTS') {
    // Set the global pipeline scoped environment (this is above each matrix)
    env.BUILD_STATED_SLACK_NOTIFIED = 'true'

    def buildType = 'Commit'
    if (params.CI_SCHEDULE != 'DO-NOT-CHANGE-THIS-SELECTION') {
      buildType = "${params.CI_SCHEDULE.toLowerCase().capitalize()}"
    }

    def color = 'good' // Green
    if (status.equalsIgnoreCase('aborted')) {
      color = '808080' // Grey
    } else if (status.equalsIgnoreCase('unstable')) {
      color = 'warning' // Orange
    } else if (status.equalsIgnoreCase('failed')) {
      color = 'danger' // Red
    }

    def message = """Build ${status} for ${env.DRIVER_DISPLAY_NAME} [${buildType}]
<${env.GITHUB_BRANCH_URL}|${env.BRANCH_NAME}> - <${env.RUN_DISPLAY_URL}|#${env.BUILD_NUMBER}> - <${env.GITHUB_COMMIT_URL}|${env.GIT_SHA}>"""
    if (!status.equalsIgnoreCase('Started')) {
      message += """
${status} after ${currentBuild.durationString - ' and counting'}"""
    }

    slackSend color: "${color}",
              channel: "#java-driver-dev-bots",
              message: "${message}"
  }
}

def submitCIMetrics(buildType) {
  long durationMs = currentBuild.duration
  long durationSec = durationMs / 1000
  long nowSec = (currentBuild.startTimeInMillis + durationMs) / 1000
  def branchNameNoPeriods = env.BRANCH_NAME.replaceAll('\\.', '_')
  def durationMetric = "okr.ci.java.${env.DRIVER_METRIC_TYPE}.${buildType}.${branchNameNoPeriods} ${durationSec} ${nowSec}"

  timeout(time: 1, unit: 'MINUTES') {
    withCredentials([string(credentialsId: 'lab-grafana-address', variable: 'LAB_GRAFANA_ADDRESS'),
                     string(credentialsId: 'lab-grafana-port', variable: 'LAB_GRAFANA_PORT')]) {
      withEnv(["DURATION_METRIC=${durationMetric}"]) {
        sh label: 'Send runtime metrics to labgrafana', script: '''#!/bin/bash -le
          echo "${DURATION_METRIC}" | nc -q 5 ${LAB_GRAFANA_ADDRESS} ${LAB_GRAFANA_PORT}
        '''
      }
    }
  }
}

def describePerCommitStage() {
  script {
    currentBuild.displayName = "Per-Commit build"
    currentBuild.description = 'Per-Commit build and testing of development Apache CassandraⓇ and current DataStax Enterprise against Oracle JDK 8'
  }
}

def describeAdhocAndScheduledTestingStage() {
  script {
    if (params.CI_SCHEDULE == 'DO-NOT-CHANGE-THIS-SELECTION') {
      // Ad-hoc build
      currentBuild.displayName = "Adhoc testing"
      currentBuild.description = "Testing ${params.ADHOC_BUILD_AND_EXECUTE_TESTS_SERVER_VERSION} against JDK version ${params.ADHOC_BUILD_AND_EXECUTE_TESTS_JABBA_VERSION}"
    } else {
      // Scheduled build
      currentBuild.displayName = "${params.CI_SCHEDULE.toLowerCase().replaceAll('_', ' ').capitalize()} schedule"
      currentBuild.description = "Testing server versions [${params.CI_SCHEDULE_SERVER_VERSIONS}] against JDK version ${params.CI_SCHEDULE_JABBA_VERSION}"
    }
  }
}

// branch pattern for cron
// should match 3.x, 4.x, 4.5.x, etc
def branchPatternCron = ~"((\\d+(\\.[\\dx]+)+))"

pipeline {
  agent none

  // Global pipeline timeout
  options {
    timeout(time: 10, unit: 'HOURS')
    buildDiscarder(logRotator(artifactNumToKeepStr: '10', // Keep only the last 10 artifacts
                              numToKeepStr: '50'))        // Keep only the last 50 build records
  }

  parameters {
    choice(
      name: 'ADHOC_BUILD_TYPE',
      choices: ['BUILD', 'BUILD-AND-EXECUTE-TESTS'],
      description: '''<p>Perform a adhoc build operation</p>
                      <table style="width:100%">
                        <col width="25%">
                        <col width="75%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                        <tr>
                          <td><strong>BUILD</strong></td>
                          <td>Performs a <b>Per-Commit</b> build</td>
                        </tr>
                        <tr>
                          <td><strong>BUILD-AND-EXECUTE-TESTS</strong></td>
                          <td>Performs a build and executes the integration and unit tests</td>
                        </tr>
                      </table>''')
    choice(
      name: 'ADHOC_BUILD_AND_EXECUTE_TESTS_SERVER_VERSION',
      choices: ['2.1',       // Legacy Apache CassandraⓇ
                '2.2',       // Legacy Apache CassandraⓇ
                '3.0',       // Previous Apache CassandraⓇ
                '3.11',      // Current Apache CassandraⓇ
                '4.0',       // Development Apache CassandraⓇ
                'dse-4.8',   // Previous EOSL DataStax Enterprise
                'dse-5.0',   // Long Term Support DataStax Enterprise
                'dse-5.1',   // Legacy DataStax Enterprise
                'dse-6.0',   // Previous DataStax Enterprise
                'dse-6.7',   // Previous DataStax Enterprise
                'dse-6.8',   // Current DataStax Enterprise
                'ALL'],
      description: '''Apache Cassandra&reg; and DataStax Enterprise server version to use for adhoc <b>BUILD-AND-EXECUTE-TESTS</b> builds
                      <table style="width:100%">
                        <col width="15%">
                        <col width="85%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                        <tr>
                          <td><strong>2.1</strong></td>
                          <td>Apache Cassandra&reg; v2.1.x</td>
                        </tr>
                        <tr>
                          <td><strong>2.2</strong></td>
                          <td>Apache Cassandra&reg; v2.2.x</td>
                        </tr>
                        <tr>
                          <td><strong>3.0</strong></td>
                          <td>Apache Cassandra&reg; v3.0.x</td>
                        </tr>
                        <tr>
                          <td><strong>3.11</strong></td>
                          <td>Apache Cassandra&reg; v3.11.x</td>
                        </tr>
                        <tr>
                          <td><strong>4.0</strong></td>
                          <td>Apache Cassandra&reg; v4.x (<b>CURRENTLY UNDER DEVELOPMENT</b>)</td>
                        </tr>
                        <tr>
                          <td><strong>dse-4.8</strong></td>
                          <td>DataStax Enterprise v4.8.x (<b>END OF SERVICE LIFE</b>)</td>
                        </tr>
                        <tr>
                          <td><strong>dse-5.0</strong></td>
                          <td>DataStax Enterprise v5.0.x (<b>Long Term Support</b>)</td>
                        </tr>
                        <tr>
                          <td><strong>dse-5.1</strong></td>
                          <td>DataStax Enterprise v5.1.x</td>
                        </tr>
                        <tr>
                          <td><strong>dse-6.0</strong></td>
                          <td>DataStax Enterprise v6.0.x</td>
                        </tr>
                        <tr>
                          <td><strong>dse-6.7</strong></td>
                          <td>DataStax Enterprise v6.7.x</td>
                        </tr>
                        <tr>
                          <td><strong>dse-6.8</strong></td>
                          <td>DataStax Enterprise v6.8.x</td>
                        </tr>
                      </table>''')
    choice(
      name: 'ADHOC_BUILD_AND_EXECUTE_TESTS_JABBA_VERSION',
      choices: ['1.8',           // Oracle JDK version 1.8 (current default)
                'openjdk@1.9',   // OpenJDK version 9
                'openjdk@1.10',  // OpenJDK version 10
                'openjdk@1.11',  // OpenJDK version 11
                'openjdk@1.12',  // OpenJDK version 12
                'openjdk@1.13',  // OpenJDK version 13
                'openjdk@1.14'], // OpenJDK version 14
      description: '''JDK version to use for <b>TESTING</b> when running adhoc <b>BUILD-AND-EXECUTE-TESTS</b> builds. <i>All builds will use JDK8 for building the driver</i>
                      <table style="width:100%">
                        <col width="15%">
                        <col width="85%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                        <tr>
                          <td><strong>1.8</strong></td>
                          <td>Oracle JDK version 1.8 (<i>Used for compiling regardless of choice</i>)</td>
                        </tr>
                        <tr>
                          <td><strong>openjdk@1.9</strong></td>
                          <td>OpenJDK version 9</td>
                        </tr>
                        <tr>
                          <td><strong>openjdk@1.10</strong></td>
                          <td>OpenJDK version 10</td>
                        </tr>
                        <tr>
                          <td><strong>openjdk@1.11</strong></td>
                          <td>OpenJDK version 11</td>
                        </tr>
                        <tr>
                          <td><strong>openjdk@1.12</strong></td>
                          <td>OpenJDK version 12</td>
                        </tr>
                        <tr>
                          <td><strong>openjdk@1.13</strong></td>
                          <td>OpenJDK version 13</td>
                        </tr>
                        <tr>
                          <td><strong>openjdk@1.14</strong></td>
                          <td>OpenJDK version 14</td>
                        </tr>
                      </table>''')
    booleanParam(
      name: 'SKIP_SERIAL_ITS',
      defaultValue: false,
      description: 'Flag to determine if serial integration tests should be skipped')
    booleanParam(
      name: 'SKIP_ISOLATED_ITS',
      defaultValue: false,
      description: 'Flag to determine if isolated integration tests should be skipped')
    booleanParam(
      name: 'SKIP_PARALLELIZABLE_ITS',
      defaultValue: false,
      description: 'Flag to determine if parallel integration tests should be skipped')
    string(
      name: 'INTEGRATION_TESTS_FILTER',
      defaultValue: '',
      description: '''<p>Run only the tests whose name match patterns</p>
                      See <a href="https://maven.apache.org/surefire/maven-failsafe-plugin/examples/single-test.html">Maven Failsafe Plugin</a> for more information on filtering integration tests''')
    choice(
      name: 'CI_SCHEDULE',
      choices: ['DO-NOT-CHANGE-THIS-SELECTION', 'WEEKNIGHTS', 'WEEKENDS', 'MONTHLY'],
      description: 'CI testing schedule to execute periodically scheduled builds and tests of the driver (<strong>DO NOT CHANGE THIS SELECTION</strong>)')
    string(
      name: 'CI_SCHEDULE_SERVER_VERSIONS',
      defaultValue: 'DO-NOT-CHANGE-THIS-SELECTION',
      description: 'CI testing server version(s) to utilize for scheduled test runs of the driver (<strong>DO NOT CHANGE THIS SELECTION</strong>)')
    string(
      name: 'CI_SCHEDULE_JABBA_VERSION',
      defaultValue: 'DO-NOT-CHANGE-THIS-SELECTION',
      description: 'CI testing JDK version(s) to utilize for scheduled test runs of the driver (<strong>DO NOT CHANGE THIS SELECTION</strong>)')
  }

  triggers {
    // schedules only run against release branches (i.e. 3.x, 4.x, 4.5.x, etc.)
    parameterizedCron(branchPatternCron.matcher(env.BRANCH_NAME).matches() ? """
      # Every weeknight (Monday - Friday) around 2:00 AM
      ### JDK8 tests against 2.1, 3.0, DSE 4.8, DSE 5.0, DSE 5.1, DSE-6.0 and DSE 6.7
      H 2 * * 1-5 %CI_SCHEDULE=WEEKNIGHTS;CI_SCHEDULE_SERVER_VERSIONS=2.1 3.0 dse-4.8 dse-5.0 dse-5.1 dse-6.0 dse-6.7;CI_SCHEDULE_JABBA_VERSION=1.8
      ### JDK11 tests against 3.11, 4.0 and DSE 6.8
      H 2 * * 1-5 %CI_SCHEDULE=WEEKNIGHTS;CI_SCHEDULE_SERVER_VERSIONS=3.11 4.0 dse-6.8;CI_SCHEDULE_JABBA_VERSION=openjdk@1.11
      # Every weekend (Sunday) around 12:00 PM noon
      ### JDK14 tests against 3.11, 4.0 and DSE 6.8
      H 12 * * 0 %CI_SCHEDULE=WEEKENDS;CI_SCHEDULE_SERVER_VERSIONS=3.11 4.0 dse-6.8;CI_SCHEDULE_JABBA_VERSION=openjdk@1.14
    """ : "")
  }

  environment {
    OS_VERSION = 'ubuntu/bionic64/java-driver'
    JABBA_SHELL = '/usr/lib/jabba/jabba.sh'
    CCM_ENVIRONMENT_SHELL = '/usr/local/bin/ccm_environment.sh'
    SERIAL_ITS_ARGUMENT = "-DskipSerialITs=${params.SKIP_SERIAL_ITS}"
    ISOLATED_ITS_ARGUMENT = "-DskipIsolatedITs=${params.SKIP_ISOLATED_ITS}"
    PARALLELIZABLE_ITS_ARGUMENT = "-DskipParallelizableITs=${params.SKIP_PARALLELIZABLE_ITS}"
    INTEGRATION_TESTS_FILTER = "${params.INTEGRATION_TESTS_FILTER}"
  }

  stages {
    stage ('Per-Commit') {
      options {
        timeout(time: 2, unit: 'HOURS')
      }
      when {
        beforeAgent true
        allOf {
          expression { params.ADHOC_BUILD_TYPE == 'BUILD' }
          expression { params.CI_SCHEDULE == 'DO-NOT-CHANGE-THIS-SELECTION' }
          expression { params.CI_SCHEDULE_SERVER_VERSIONS == 'DO-NOT-CHANGE-THIS-SELECTION' }
          expression { params.CI_SCHEDULE_JABBA_VERSION == 'DO-NOT-CHANGE-THIS-SELECTION' }
          not { buildingTag() }
        }
      }

      matrix {
        axes {
          axis {
            name 'SERVER_VERSION'
            values '3.11',     // Latest stable Apache CassandraⓇ
                   '4.0',      // Development Apache CassandraⓇ
                   'dse-6.8' // Current DataStax Enterprise
          }
        }

        agent {
          label "${OS_VERSION}"
        }
        environment {
          // Per-commit builds are only going to run against JDK8
          JABBA_VERSION = '1.8'
        }

        stages {
          stage('Initialize-Environment') {
            steps {
              initializeEnvironment()
              script {
                if (env.BUILD_STATED_SLACK_NOTIFIED != 'true') {
                  notifySlack()
                }
              }
            }
          }
          stage('Describe-Build') {
            steps {
              describePerCommitStage()
            }
          }
          stage('Build-Driver') {
            steps {
              buildDriver(env.JABBA_VERSION)
            }
          }
          stage('Execute-Tests') {
            steps {
              catchError {
                // Use the matrix JDK for testing
                executeTests()
              }
            }
            post {
              always {
                /*
                 * Empty results are possible
                 *
                 *  - Build failures during mvn verify may exist so report may not be available
                 */
                junit testResults: '**/target/surefire-reports/TEST-*.xml', allowEmptyResults: true
                junit testResults: '**/target/failsafe-reports/TEST-*.xml', allowEmptyResults: true
              }
            }
          }
          stage('Execute-Code-Coverage') {
            // Ensure the code coverage is run only once per-commit
            when { environment name: 'SERVER_VERSION', value: '4.0' }
            steps {
              executeCodeCoverage()
            }
          }
        }
      }
      post {
        always {
          node('master') {
            submitCIMetrics('commit')
          }
        }
        aborted {
          notifySlack('aborted')
        }
        success {
          notifySlack('completed')
        }
        unstable {
          notifySlack('unstable')
        }
        failure {
          notifySlack('FAILED')
        }
      }
    }

    stage('Adhoc-And-Scheduled-Testing') {
      when {
        beforeAgent true
        allOf {
          expression { (params.ADHOC_BUILD_TYPE == 'BUILD' && params.CI_SCHEDULE != 'DO-NOT-CHANGE-THIS-SELECTION') ||
                       params.ADHOC_BUILD_TYPE == 'BUILD-AND-EXECUTE-TESTS' }
          not { buildingTag() }
          anyOf {
            expression { params.ADHOC_BUILD_TYPE == 'BUILD-AND-EXECUTE-TESTS' }
            allOf {
              expression { params.ADHOC_BUILD_TYPE == 'BUILD' }
              expression { params.CI_SCHEDULE != 'DO-NOT-CHANGE-THIS-SELECTION' }
              expression { params.CI_SCHEDULE_SERVER_VERSIONS != 'DO-NOT-CHANGE-THIS-SELECTION' }
            }
          }
        }
      }

      environment {
        SERVER_VERSIONS = "${params.CI_SCHEDULE_SERVER_VERSIONS == 'DO-NOT-CHANGE-THIS-SELECTION' ? params.ADHOC_BUILD_AND_EXECUTE_TESTS_SERVER_VERSION : params.CI_SCHEDULE_SERVER_VERSIONS}"
        JABBA_VERSION = "${params.CI_SCHEDULE_JABBA_VERSION == 'DO-NOT-CHANGE-THIS-SELECTION' ? params.ADHOC_BUILD_AND_EXECUTE_TESTS_JABBA_VERSION : params.CI_SCHEDULE_JABBA_VERSION}"
      }

      matrix {
        axes {
          axis {
            name 'SERVER_VERSION'
            values '2.1',       // Legacy Apache CassandraⓇ
                   '3.0',       // Previous Apache CassandraⓇ
                   '3.11',      // Current Apache CassandraⓇ
                   '4.0',       // Development Apache CassandraⓇ
                   'dse-4.8',   // Previous EOSL DataStax Enterprise
                   'dse-5.0',   // Last EOSL DataStax Enterprise
                   'dse-5.1',   // Legacy DataStax Enterprise
                   'dse-6.0',   // Previous DataStax Enterprise
                   'dse-6.7',   // Previous DataStax Enterprise
                   'dse-6.8'    // Current DataStax Enterprise
          }
        }
        when {
          beforeAgent true
          allOf {
            expression { return env.SERVER_VERSIONS.split(' ').any { it =~ /(ALL|${env.SERVER_VERSION})/ } }
          }
        }
        agent {
          label "${env.OS_VERSION}"
        }

        stages {
          stage('Initialize-Environment') {
            steps {
              initializeEnvironment()
              script {
                if (env.BUILD_STATED_SLACK_NOTIFIED != 'true') {
                  notifySlack()
                }
              }
            }
          }
          stage('Describe-Build') {
            steps {
              describeAdhocAndScheduledTestingStage()
            }
          }
          stage('Build-Driver') {
            steps {
              // Jabba default should be a JDK8 for now
              buildDriver('default')
            }
          }
          stage('Execute-Tests') {
            steps {
              catchError {
                // Use the matrix JDK for testing
                executeTests()
              }
            }
            post {
              always {
                /*
                 * Empty results are possible
                 *
                 *  - Build failures during mvn verify may exist so report may not be available
                 *  - With boolean parameters to skip tests a failsafe report may not be available
                 */
                junit testResults: '**/target/surefire-reports/TEST-*.xml', allowEmptyResults: true
                junit testResults: '**/target/failsafe-reports/TEST-*.xml', allowEmptyResults: true
              }
            }
          }
          stage('Execute-Code-Coverage') {
            // Ensure the code coverage is run only once per-commit
            when {
              allOf {
                environment name: 'SERVER_VERSION', value: '4.0'
                environment name: 'JABBA_VERSION', value: '1.8'
              }
            }
            steps {
              executeCodeCoverage()
            }
          }
        }
      }
      post {
        aborted {
          notifySlack('aborted')
        }
        success {
          notifySlack('completed')
        }
        unstable {
          notifySlack('unstable')
        }
        failure {
          notifySlack('FAILED')
        }
      }
    }
  }
}
