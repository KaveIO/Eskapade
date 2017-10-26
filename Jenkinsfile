pipeline {
    agent any
    stages {
        stage('Test') {
            steps {
                echo 'Setting up environment and testing...'
                sh '''
                    source /opt/KaveToolbox/pro/scripts/KaveEnv.sh
                    pip install tox
                    if [[ ":$LD_LIBRARY_PATH:" != *"$(dirname $(which python))/../lib:"* ]]; then
                        export LD_LIBRARY_PATH="$(dirname $(which python))/../lib:${LD_LIBRARY_PATH}"
                    fi
                    tox -r
                '''
            }
        }
    }
}