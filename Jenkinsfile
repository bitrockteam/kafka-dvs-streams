pipeline {
    agent any
    environment {
        DOCKER_REPOSITORY = '618624782178.dkr.ecr.eu-west-1.amazonaws.com'
        AWS_REGION = 'eu-west-1'
        GITHUB_CREDENTIALS = 'BitrockCI token'
        GITHUB_ACCOUNT = 'bitrockteam'
        GITHUB_REPO = 'kafka-flightstream-streams'
        GITHUB_SSH = "centos"
        RELEASE_BRANCH = "master"
        SBT_OPTS="-Xmx2048M"
    }
    options {
        ansiColor('xterm')
    }
    stages {
        stage("Branch checkout") {
            steps {
                script {
                    sh "git checkout ${BRANCH_NAME}"
                    committerEmail = sh(
                            script: "git --no-pager show -s --format=\'%ae\'",
                            returnStdout: true
                    ).trim()
                }
            }
        }
        stage("Environment setup") {
            when {
                not {
                    equals expected: "ci@bitrock.it", actual: committerEmail
                }
            }
            steps {
                githubNotify status: "PENDING",
                        credentialsId: GITHUB_CREDENTIALS,
                        description: "Build pending",
                        account: GITHUB_ACCOUNT,
                        repo: GITHUB_REPO,
                        sha: GIT_COMMIT
                sh "git config --local user.name Jenkins"
                sh "git config --local user.email ci@bitrock.it"
                sh """
                    set +x
                    \$(aws ecr get-login --no-include-email --region ${AWS_REGION})
                    set -x
                    """
                script {
                    tagBefore = sh(
                            script: "git describe --tags --abbrev=0 | sed 's/^v//'",
                            returnStdout: true
                    ).trim()
                }
            }
        }
        stage("Building master release") {
            when {
                allOf {
                    branch RELEASE_BRANCH
                    not {
                        equals expected: "ci@bitrock.it", actual: committerEmail
                    }
                }
            }
            steps {
                echo "Building master branch"
                sshagent (credentials: ['centos']) {
                    withCredentials([usernamePassword(credentialsId: 'BitrockNexus',
                            usernameVariable: 'NEXUS_USER',
                            passwordVariable: 'NEXUS_PASSWORD')]) {
                        sh """
                        mkdir .sbt
                        echo "realm=Sonatype Nexus Repository Manager" > .sbt/.credentials
                        echo "host=nexus.reactive-labs.io" >> .sbt/.credentials
                        echo "user=${NEXUS_USER}" >> .sbt/.credentials
                        echo "password=${NEXUS_PASSWORD}" >> .sbt/.credentials
                        """
                        sh "sbt 'release with-defaults'"
                        githubNotify status: "SUCCESS",
                                credentialsId: GITHUB_CREDENTIALS,
                                description: "Build success",
                                account: GITHUB_ACCOUNT,
                                repo: GITHUB_REPO,
                                sha: GIT_COMMIT

                        script {
                            tagAfter = sh(
                                    script: "git describe --tags --abbrev=0 | sed 's/^v//'",
                                    returnStdout: true
                            ).trim()
                        }

                        slackSend color: "#008000",
                                message: ":star-struck: ${JOB_NAME} released ${tagAfter}! (<${BUILD_URL}|Open>)"

                        sh "docker push ${DOCKER_REPOSITORY}/${GITHUB_REPO}:${tagAfter}"
                        sh "docker tag ${DOCKER_REPOSITORY}/${GITHUB_REPO}:${tagAfter} ${DOCKER_REPOSITORY}/${GITHUB_REPO}:latest"
                        sh "docker push ${DOCKER_REPOSITORY}/${GITHUB_REPO}:latest"
                        sh "printf '[{\"name\":\"kafka-geostream-streams\",\"imageUri\":\"%s\"}]' \$(git describe --tags --abbrev=0 | sed 's/^v//') > imagedefinitions.json"
                    }
                }
            }
        }
        stage("Trigger deploy") {
            when {
                allOf {
                    branch RELEASE_BRANCH
                    not {
                        equals expected: "ci@bitrock.it", actual: committerEmail
                    }
                }
            }
            steps {
		build job: 'kafka-geostream-cd/master', wait: false
	    }
 	}
        stage("Building feature/develop") {
            when {
                allOf {
                    not {
                        branch RELEASE_BRANCH
                    }
                    not {
                        equals expected: "ci@bitrock.it", actual: committerEmail
                    }
                }
            }
            steps {
                echo "Building feature/develop branch"
                withCredentials([usernamePassword(credentialsId: 'BitrockNexus',
                        usernameVariable: 'NEXUS_USER',
                        passwordVariable: 'NEXUS_PASSWORD')]) {
                    sh """
                        mkdir .sbt
                        echo "realm=Sonatype Nexus Repository Manager" > .sbt/.credentials
                        echo "host=nexus.reactive-labs.io" >> .sbt/.credentials
                        echo "user=${NEXUS_USER}" >> .sbt/.credentials
                        echo "password=${NEXUS_PASSWORD}" >> .sbt/.credentials
                    """
                    sh "sbt test docker:publishLocal docker:clean"
                    githubNotify status: "SUCCESS",
                            credentialsId: GITHUB_CREDENTIALS,
                            description: "Build success",
                            account: GITHUB_ACCOUNT,
                            repo: GITHUB_REPO,
                            sha: GIT_COMMIT
                }
            }
        }
    }

    post {
        always {
            echo "Cleaning workspace"
            cleanWs()
        }
        failure {
            githubNotify status: "FAILURE",
                    credentialsId: GITHUB_CREDENTIALS,
                    description: "Build failure",
                    account: GITHUB_ACCOUNT,
                    repo: GITHUB_REPO,
                    sha: GIT_COMMIT

            script {
                if (tagBefore) {
                    sh "docker pull ${DOCKER_REPOSITORY}/${GITHUB_REPO}:${tagBefore}"
                    sh "docker tag ${DOCKER_REPOSITORY}/${GITHUB_REPO}:${tagBefore} ${DOCKER_REPOSITORY}/${GITHUB_REPO}:latest"
                    sh "docker push ${DOCKER_REPOSITORY}/${GITHUB_REPO}:latest"
                    sh "aws ecr batch-delete-image --repository-name ${DOCKER_REPOSITORY}/${GITHUB_REPO} --image-ids imageTag=${tagAfter} --region ${AWS_region} || true"
                }
            }

            slackSend color: "#FF0000",
                    message: ":exploding_head: ${JOB_NAME} build ${BUILD_NUMBER} failure! (<${BUILD_URL}|Open>)"
        }
    }
}
