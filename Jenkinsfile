pipeline {
  agent any
  stages {
    stage('Build') {
      steps {
        echo 'Building..'
        sh 'ci/build.sh'
      }
    }
    stage('Test') {
      steps {
        echo 'Testing..'
        sh 'ci/test_java_embedded.sh'
      }
    }
    stage('Deploy') {
      steps {
        echo 'Deploying....'
      }
    }
  }
}
