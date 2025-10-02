@Library('shared-libraries') _

def runtests(String marklogicVersion) {
  cleanupDocker()
  sh label:'mlsetup', script: '''#!/bin/bash
    echo "Removing any running MarkLogic server and clean up MarkLogic data directory"
    sudo /usr/local/sbin/mladmin remove
    docker-compose down -v || true
    sudo /usr/local/sbin/mladmin cleandata
    cd kafka-connector
    MARKLOGIC_LOGS_VOLUME=/tmp MARKLOGIC_IMAGE='''+marklogicVersion+''' docker-compose up -d --build
    sleep 120s;
  '''
  sh label:'deploy project', script: '''#!/bin/bash
    export JAVA_HOME=$JAVA17_HOME_DIR
    export GRADLE_USER_HOME=$WORKSPACE/$GRADLE_DIR
    export PATH=$GRADLE_USER_HOME:$JAVA_HOME/bin:$PATH
    cd kafka-connector
    ./gradlew hubInit
    ./gradlew -i mlDeploy
  '''
  sh label:'test', script: '''#!/bin/bash
    export JAVA_HOME=$JAVA17_HOME_DIR
    export GRADLE_USER_HOME=$WORKSPACE/$GRADLE_DIR
    export PATH=$GRADLE_USER_HOME:$JAVA_HOME/bin:$PATH
    cd kafka-connector
    ./gradlew test  || true
  '''
  junit '**/build/**/*.xml'
}

pipeline{
  agent {label 'devExpLinuxPool'}
  options {
    checkoutToSubdirectory 'kafka-connector'
    buildDiscarder logRotator(artifactDaysToKeepStr: '7', artifactNumToKeepStr: '', daysToKeepStr: '30', numToKeepStr: '')
  }
  environment{
    JAVA17_HOME_DIR="/home/builder/java/jdk-17.0.2"
    GRADLE_DIR   =".gradle"
  }
  stages{
    stage('test-ML12'){
      steps{
        runtests("ml-docker-db-dev-tierpoint.bed-artifactory.bedford.progress.com/marklogic/marklogic-server-ubi-rootless:12.1.nightly-ubi-rootless")
      }
      post{
        always{
          updateWorkspacePermissions()
          sh label:'mlcleanup', script: '''#!/bin/bash
            cd kafka-connector
            docker-compose down -v || true
          '''
          cleanupDocker()
        }
      }
    }
    stage('test-ML11'){
      steps{
        runtests("ml-docker-db-dev-tierpoint.bed-artifactory.bedford.progress.com/marklogic/marklogic-server-ubi:latest-11")
      }
      post{
        always{
          updateWorkspacePermissions()
          sh label:'mlcleanup', script: '''#!/bin/bash
            cd kafka-connector
            docker-compose down -v || true
          '''
          cleanupDocker()
        }
      }
    }
  }
}
