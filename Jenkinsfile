pipeline {
    agent any
    environment {
        SPARK_HOME = '/usr/hdp/current/spark2-client'
        SPARK_JARS_DIR = '$SPARK_HOME/jars'
        SPARK_MAJOR_VERSION = 2
        HDP_VERSION = '2.6.1.0-129'
    }
    stages {
        stage('Test') {
            steps {
                echo 'Setting up environment and testing...'
                withEnv(["PATH+SPARK=${env.SPARK_HOME}/bin"]) {
                    sh '''
                        export PYTHONPATH=${SPARK_HOME}"/python:"${PYTHONPATH}
                        if [[ ":$PYTHONPATH:" != *"$(ls ${SPARK_HOME}/python/lib/py4j-*-src.zip):"* ]]; then
                            export PYTHONPATH="$(ls ${SPARK_HOME}/python/lib/py4j-*-src.zip):${PYTHONPATH}"
                        fi
                        source /opt/anaconda/pro/bin/activate
                        source /opt/root/pro/bin/thisroot.sh
                        if [[ ":$LD_LIBRARY_PATH:" != *"$(dirname $(which python))/../lib:"* ]]; then
                            export LD_LIBRARY_PATH="$(dirname $(which python))/../lib:${LD_LIBRARY_PATH}"
                        fi
                        tox -r
                    '''
                }
            }
        }
    }
    post {
        always {
            echo 'Cleaning up...'
            sh ('rm -rf /tmp/eskapade_stream_test')
            sh ('rm -rf MyPdfV3.*')
            sh ('rm -rf MyPdfV3_cxx*')
        }
    }
}