pipeline {
  agent { label 'streamsx_public' }
  environment {
     VCAP_SERVICES = credentials('VCAP_SERVICES')
     STREAMING_ANALYTICS_SERVICE_NAME = credentials('STREAMING_ANALYTICS_SERVICE_NAME')
  }
  stages {
    stage('Build') {
      steps {
        sh 'ci/build.sh'
      }
    }
    stage('Java/Scala embedded') {
       steps {
         sh 'ci/test_java_embedded.sh'
       }
    }
    stage('Java/Scala standalone') {
       steps {
         sh 'ci/test_java_standalone.sh'
       }
    }
    stage('Python 3.6 standalone') {
       steps {
         script {
           try {
             sh 'ci/test_python36_standalone.sh'
           }
           catch (exc) {
             currentBuild.result = 'UNSTABLE'
           }
         }
       }
    }
    stage('Python 3.5 standalone') {
       when { anyOf { branch 'master'; branch 'feature/*' } }
       steps {
         script {
           try {
             sh 'ci/test_python35_standalone.sh'
           }
           catch (exc) {
             currentBuild.result = 'UNSTABLE'
           }
         }
       }
    }
    stage('Python 3.5 Streaming Analytics') {
       when { anyOf { branch 'master'; branch 'feature/*' } }
       steps {
         script {
           try {
             sh 'ci/test_python35_service.sh'
           }
           catch (exc) {
             currentBuild.result = 'UNSTABLE'
           }
         }
       }
    }
    stage('Python 2.7 standalone') {
       when { anyOf { branch 'master'; branch 'feature/*' } }
       steps {
         script {
           try {
               sh 'ci/test_python27_standalone.sh'
           }
           catch (exc) {
             currentBuild.result = 'UNSTABLE'
           }
         }
       }
    }
  }
  post {
    always {
      junit "test/**/TEST-*.xml"
      publishHTML (target: [
          reportName: 'Java Coverage',
          reportDir: 'test/java/report/coverage',
          reportFiles: 'index.html',
          keepAll: false,
          alwaysLinkToLastBuild: true,
          allowMissing: true
      ])
      publishHTML (target: [
          reportName: 'Python 2.7 Coverage',
          reportDir: 'test/python/nose_runs/py27/coverage',
          reportFiles: 'index.html',
          keepAll: false,
          alwaysLinkToLastBuild: true,
          allowMissing: true
      ])
      publishHTML (target: [
          reportName: 'Python 3.6 Coverage',
          reportDir: 'test/python/nose_runs/py36/coverage',
          reportFiles: 'index.html',
          keepAll: false,
          alwaysLinkToLastBuild: true,
          allowMissing: true
      ])
    }
    fixed {
      slackSend (color: 'good', message: "FIXED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
    }
    unstable {
      slackSend (color: 'warning', message: "UNSTABLE: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
    }
    failure {
      slackSend (color: 'danger', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
    }
  }
}
