pipeline {
  agent { label 'streamsx_public' }
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
         sh 'ci/test_python36_standalone.sh'
       }
    }
    stage('Python 3.5 standalone') {
       when { anyOf { branch 'DISABLE_master'; branch 'DISABLE_feature/*' } }
       steps {
         sh 'ci/test_python35_standalone.sh'
       }
    }
    stage('Python 2.7 standalone') {
       when { anyOf { branch 'master'; branch 'feature/*' } }
       steps {
         sh 'ci/test_python27_standalone.sh'
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
  }
}
