pipeline {
  agent none
  stages {
    stage('Build') {
      agent any
      steps {
        sh 'ci/build.sh'
      }
    }
    stage('Test') {
      parallel {
        stage('Java/Scala embedded') {
          agent any
          steps {
            sh 'ci/test_java_embedded.sh'
          }
        }
        stage('Java/Scala standalone') {
          agent any
          steps {
            sh 'ci/test_java_standalone.sh'
          }
        }
      }
    }
  }
  post {
    always {
      junit "test/java/unittests/*/TEST-*.xml"
    }
  }
}
