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
    if (msg.empty) {
        result
    } else {
        msg += "\n${result}"
    }
}


podTemplate(
    name: pod_name,
    label: pod_label,
    namespace: 'jenkins',
    containers: [
        containerTemplate(name: 'jnlp',
                          image: 'kave/jnlp-slave-kavetoolbox',
                          workingDir: '/home/jenkins',
                          args: '${computer.jnlpmac} ${computer.name}',
                          envVars: [ envVar(key: 'JENKINS_URL', value: 'http://its-jenkins:8080')
            ]
        )
    ]
) {
    node(pod_label) {

        def status_msg

        stage('Setup') {
            try {
                echo "Creating and setting up Python virtual environment for ${env.JOB_NAME}:${env.BRANCH_NAME}."

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
                // on the slave.
                container(name: 'jnlp', shell: '/bin/bash') {
                    status_msg = sh(returnStdout: true, script: to_execute).trim()
                }

                currentBuild.result = 'SUCCESS'
                echo "${status_string(status_msg, currentBuild.result)}"
            } catch (exc) {
                echo 'Failed to setup project environment!'
                echo exc.toString()
                currentBuild.result = 'FAILURE'
            }
        }

        status_msg = ''
        stage('Checkout') {
            try {
                echo "Going to checkout ${env.JOB_NAME}:${env.BRANCH_NAME}"
                container(name: 'jnlp', shell: '/bin/bash') {
                    checkout scm
                    sh 'pwd && ls -l'
                }

                currentBuild.result = 'SUCCESS'
                echo "${status_string(status_msg, currentBuild.result)}"
            } catch (exc) {
                echo 'Failed to checkout source!'
                echo exc.toString()
                currentBuild.result = 'FAILURE'
            }
        }
    }
}
