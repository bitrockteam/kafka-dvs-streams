pipeline {
    agent any
    environment {
        DOCKER_REPOSITORY = '618624782178.dkr.ecr.eu-west-1.amazonaws.com'
        AWS_REGION = 'eu-west-1'
        GITHUB_CREDENTIALS = 'BitrockCI token'
        GITHUB_ACCOUNT = 'bitrockteam'
        GITHUB_REPO = 'kafka-dvs-streams'
        GITHUB_SSH = "centos"
        RELEASE_BRANCH = "master"
        SBT_OPTS="-Xmx2048M"
        AWS_CREDENTIALS=""
    }
    options {
        ansiColor('xterm')
    }
    stages {
        stage("Branch checkout") {
            steps {
                checkout([
                    $class: 'GitSCM',
                    branches: scm.branches,
                    extensions: scm.extensions + [[$class: 'CleanCheckout'], [$class: 'LocalBranch', localBranch: ''],
                       [$class: 'CloneOption', depth: 2, shallow: false]],
                    userRemoteConfigs: scm.userRemoteConfigs
                ])
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
                    AWS_CREDENTIALS = sh (
                        script: "aws ecr get-login --no-include-email --region ${AWS_REGION}",
                        returnStdout: true
                    ).trim()
                    tagBefore = sh(
                            script: "git describe --tags --abbrev=0 | sed 's/^v//'",
                            returnStdout: true
                    ).trim()
                }
            }
        }
        stage("Building master release") {
            agent {
                dockerfile {
                     filename 'Dockerfile.build'
                     args '--group-add 994 -v /var/run/docker.sock:/var/run/docker.sock -v /var/lib/jenkins/.cache/coursier/v1:/sbt-cache/.cache/coursier/v1 -v /var/lib/jenkins/.sbtboot:/sbt-cache/.sbtboot -v /var/lib/jenkins/.sbt/boot/:/sbt-cache/.boot -v /var/lib/jenkins/.ivy2:/sbt-cache/.ivy2'
                }
            }
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
                    checkout([
                         $class: 'GitSCM',
                         branches: scm.branches,
                         extensions: scm.extensions + [[$class: 'CleanCheckout'], [$class: 'LocalBranch', localBranch: ''],
                            [$class: 'CloneOption', depth: 2, shallow: false]],
                         userRemoteConfigs: scm.userRemoteConfigs
                    ])
                    sh """
                        set +x
                        ${AWS_CREDENTIALS}
                        set -x
                        git checkout ${BRANCH_NAME}
                        git config remote.origin.fetch +refs/heads/*:refs/remotes/origin/*
                        git config branch.${BRANCH_NAME}.remote origin
                        git config branch.${BRANCH_NAME}.merge refs/heads/${BRANCH_NAME}
                        """
                    sh "sbt -Dsbt.global.base=.sbt -Dsbt.boot.directory=.sbt -Dsbt.ivy.home=.ivy2 'release with-defaults'"
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
                    sh "docker rmi ${DOCKER_REPOSITORY}/${GITHUB_REPO}:latest"
                    sh "docker rmi ${DOCKER_REPOSITORY}/${GITHUB_REPO}:${tagAfter}"
                    sh "printf '[{\"name\":\"kafka-dvs-streams\",\"imageUri\":\"%s\"}]' \$(git describe --tags --abbrev=0 | sed 's/^v//') > imagedefinitions.json"
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
               build job: 'kafka-dvs-cd/master',
                            parameters: [[$class: 'StringParameterValue', name: 'deployment', value: "${GITHUB_REPO}@${tagAfter}"]],
                            wait: false
	    }
 	}
        stage("Building feature/develop") {
             agent {
                dockerfile {
                     filename 'Dockerfile.build'
                     args '--group-add 994 -v /var/run/docker.sock:/var/run/docker.sock -v /var/lib/jenkins/.cache/coursier/v1:/sbt-cache/.cache/coursier/v1 -v /var/lib/jenkins/.sbtboot:/sbt-cache/.sbtboot -v /var/lib/jenkins/.sbt/boot/:/sbt-cache/.boot -v /var/lib/jenkins/.ivy2:/sbt-cache/.ivy2'
                }
            }
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
                sh "sbt -Dsbt.global.base=.sbt -Dsbt.boot.directory=.sbt -Dsbt.ivy.home=.ivy2 fixCheck test docker:publishLocal docker:clean"
                githubNotify status: "SUCCESS",
                        credentialsId: GITHUB_CREDENTIALS,
                        description: "Build success",
                        account: GITHUB_ACCOUNT,
                        repo: GITHUB_REPO,
                        sha: GIT_COMMIT
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
