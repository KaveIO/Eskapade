def pod_name = 'kavetoolbox'
def pod_label = "${pod_name}-jenkins-slave-${UUID.randomUUID().toString()}"
def jenkinsenv = 'jenkinsenv'

def python_packages = [ 'pip', 'virtualenv', 'tox']

// Little helper to generate a status message for a
// stage. Also, serves as an Froovy method/string
// templating example.
//
// Note that Groovy always returns the last evaluated
// statement.
def status_string(String msg, String result) {
    if (!msg.empty) {
        msg += "\n"
        msg += result
        return msg
    } else {
        return result
    }
}


podTemplate(
    cloud: 'kubernetes',
    name: pod_name,
    label: pod_label,
    namespace: 'jenkins',
    nodeUsageMode: 'EXCLUSIVE',
    containers: [
        containerTemplate(name: 'jnlp',
                          image: 'kave/jnlp-slave-kavetoolbox',
                          alwaysPullImage: true,
                          workingDir: '/home/jenkins',
                          args: '${computer.jnlpmac} ${computer.name}',
                          envVars: [ envVar(key: 'JENKINS_URL', value: 'http://its-jenkins:8080')]
        ),
        containerTemplate(name: 'docker',
                          image: 'docker',
                          alwaysPullImage: true,
                          command: 'cat',
                          ttyEnabled: true
        )
    ]
) {
    node(pod_label) {

        def status_msg = ''

        stage('Setup') {

            try {
                // Clean up workspace
                deleteDir()

                echo "Creating and setting up Python virtual environment for ${env.JOB_NAME}."

                // Build the shell script to setup and create the
                // virtual environment. This will be executed by
                // Jenkins on the slave.
                def to_execute = "#!/bin/bash\n" +
                    "conda create --name ${jenkinsenv} -y\n" +
                    "source activate jenkinsenv\n" +
                    "conda install pip -y\n"

                python_packages.each {
                    to_execute += "pip install --upgrade ${it}\n"
                }

                // Let's execute the script and capture the output
                // on the jnlp slave. By default everything is executed
                // on the jnlp slave.
                status_msg = sh(returnStdout: true, script: to_execute).trim()

                echo status_string(status_msg, 'SUCCESS')
                currentBuild.result = 'SUCCESS'
            } catch (exc) {
                // Abort! It does not make sense to continue!
                status_msg = "Failed to create and setup project environment for ${env.JOB_NAME}!\n"
                status_msg += exc.toString()
                currentBuild.result = 'ABORT'
                error status_msg
            }
        }

        stage('Checkout') {
            status_msg = ''
            try {
                echo "Going to checkout ${env.JOB_NAME}."

                checkout scm

                echo status_string(status_msg, 'SUCCESS')
                currentBuild.result = 'SUCCESS'
            } catch (exc) {
                // Abort! It does not make sense to continue!
                status_msg = "Failed to checkout ${env.JOB_NAME}!\n"
                status_msg exc.toString()
                currentBuild.result = 'ABORT'
                error status_msg
            }
        }

        stage('Unit Test') {
            status_msg = ''
            try {
                echo "Going to run unit tests for ${env.JOB_NAME}."

                // Build shell script to run unit tests.
                //
                // Note that we first need to activate the
                // Python virtualenv that was created in the
                // Setup stage.
                def to_execute = "#!/bin/bash\n" +
                    "source activate jenkinsenv\n" +
                    "tox\n"

                status_msg = sh(returnStdout: true, script: to_execute).trim()

                echo status_string(status_msg, 'SUCCESS')
                currentBuild.result = 'SUCCESS'
            } catch (exc) {
                // We do not abort here. We mark this stage as a failure and continue
                // with the cleanup stage.
                status_msg = "Unit tests failed for ${env.JOB_NAME}!\n"
                status_msg += exc.toString()
                echo status_msg
                currentBuild.result = 'FAILURE'
            }
        }

        stage('Build Docker') {
            // This is where we build the eskapade docker image
            // and push it to docker hub.
            // Google for docker jenkins pipeline kubernetes for inspiration.
            echo 'Going to build docker image.'
            container(name: 'docker', shell: '/bin/bash') {
                echo 'Hello from docker container.'
            }
        }

        stage('Cleanup') {
            status_msg = ''
            try {
                echo "Going to gather test results and reports for ${env.JOB_NAME}."

                // Gather unit testing reports. We need the junit plugin
                // to display them.
                junit '**/junit-*.xml'

                // Gather coverage reports. We need the Cobertura plugin
                // to display them.
                try {
                    step([$class: 'CoberturaPublisher', coberturaReportFile: '**/coverage.xml'])
                } catch (exc) {
                    echo 'Missing Cobertura plugin for coverage reports.'
                }

                currentBuild.result = 'SUCCESS'
            } catch (exc) {
                echo "Failed to gather test resutls and reports for ${env.JOB_NAME}!"
                currentBuild.result = 'FAILURE'
            } finally {
                // Cleanup workspace
                echo "Cleaning up workspace."
                deleteDir()
            }
        }
    }
}
