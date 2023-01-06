@Library('shared-libraries') _
pipeline{
  agent {label 'devExpLinuxPool'}
  options {
    checkoutToSubdirectory 'kafka-connector'
    buildDiscarder logRotator(artifactDaysToKeepStr: '7', artifactNumToKeepStr: '', daysToKeepStr: '30', numToKeepStr: '')
  }
  environment{
    JAVA_HOME_DIR="/home/builder/java/openjdk-1.8.0-262"
    GRADLE_DIR   =".gradle"
    DMC_USER     = credentials('MLBUILD_USER')
    DMC_PASSWORD = credentials('MLBUILD_PASSWORD')
  }
  stages{
    stage('tests'){
      steps{
        copyRPM 'Latest','11.0'
        setUpML '$WORKSPACE/xdmp/src/Mark*.rpm'
        sh label:'setup', script: '''#!/bin/bash
        cd kafka-connector
        echo mlPassword=admin >> gradle-local.properties
        '''
        sh label:'deploy project', script: '''#!/bin/bash
          export JAVA_HOME=$JAVA_HOME_DIR
          export GRADLE_USER_HOME=$WORKSPACE/$GRADLE_DIR
          export PATH=$GRADLE_USER_HOME:$JAVA_HOME/bin:$PATH
          cd kafka-connector
          ./gradlew mlDeploy
        '''
        sh label:'test', script: '''#!/bin/bash
          export JAVA_HOME=$JAVA_HOME_DIR
          export GRADLE_USER_HOME=$WORKSPACE/$GRADLE_DIR
          export PATH=$GRADLE_USER_HOME:$JAVA_HOME/bin:$PATH
          cd kafka-connector
          ./gradlew test  || true
        '''
        junit '**/build/**/*.xml'
      }
    }
  }
}
