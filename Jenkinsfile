// Define base params
def git_username = "cloudspliceci"
def git_email = "build@splicemachine.com"
def vault_addr="https://vault.build.splicemachine-dev.io"

// Launch the docker container
node('dind-compose') {

    def dockerlogin = [
        [$class: 'VaultSecret', path: "secret/team/docker_hub", secretValues: [
            [$class: 'VaultSecretValue', envVar: 'username', vaultKey: 'username'],
            [$class: 'VaultSecretValue', envVar: 'password', vaultKey: 'password']]]
    ]

    def gitlogin = [
        [$class: 'VaultSecret', path: "secret/team/git_hub_ssh", secretValues: [
            [$class: 'VaultSecretValue', envVar: 'git_ssh', vaultKey: 'id_rsa']]]
    ]

    def vaultlogin = [
        [$class: 'VaultSecret', path: "secret/team/vault_jenkins", secretValues: [
            [$class: 'VaultSecretValue', envVar: 'vault_token', vaultKey: 'token']]]
    ]

    environment {
        VAULT_ADDR = "https://vault.build.splicemachine-dev.io"
        VAULT_TOKEN = "$vault_token"
    }

    try {

    notifyBuild('STARTED')

    stage('Checkout') {
      // Checkout code from repository
      checkout([  
              $class: 'GitSCM', 
              branches: [[name: '*/dev']],
              doGenerateSubmoduleConfigurations: false, 
              extensions: [[$class: 'RelativeTargetDirectory', relativeTargetDir: 'dbaas-infrastructure']], 
              submoduleCfg: [], 
              userRemoteConfigs: [[credentialsId: '88647ede-744a-444b-8c08-8313cc137944', url: 'https://github.com/splicemachine/dbaas-infrastructure.git']]
          ])
       checkout([  
              $class: 'GitSCM', 
              branches: [[name: '*/master']],
              doGenerateSubmoduleConfigurations: false, 
              extensions: [[$class: 'RelativeTargetDirectory', relativeTargetDir: 'ml-workflow']], 
              submoduleCfg: [], 
              userRemoteConfigs: [[credentialsId: '88647ede-744a-444b-8c08-8313cc137944', url: 'https://github.com/splicemachine/ml-workflow.git']]
          ])
    }

    
    // Login to docker hub in the container
    stage('Login') {
        wrap([$class: 'VaultBuildWrapper', vaultSecrets: dockerlogin]) {
            sh "docker login -u $username -p $password"
        }
    }

    stage('Prep Image') {
        dir('ml-workflow'){
            sh "python3 update_tag.py \$(pwd)/docker-compose.yaml"
            sh "cat docker-compose.yaml"
        }
    }
    

    stage('Build ML Images') {
        dir('ml-workflow'){
            sh "nohup dockerd >/dev/null 2>&1 &"
            sh "docker-compose build mlflow bobby feature_store"
            sh "docker-compose push mlflow bobby feature_store"
            sh  """
            git config --global user.email "build@splicemachine.com"
            git config --global user.name "cloudspliceci"
            git status
            git add docker-compose.yaml
            git commit -m 'Update image tag to ${BUILD_NUMBER}'
            git push
            """
        }
    }

    stage('Edit dbaas-infrastructure') {
        dir('dbaas-infrastructure'){
            sh "python3 ../ml-workflow/update_tag.py \$(pwd)/kubernetes/charts/splice/values.yaml"
            sh "cat \$(pwd)/kubernetes/charts/splice/values.yaml"
            sh """
            git config --global user.email "build@splicemachine.com"
            git config --global user.name "cloudspliceci"
            git checkout -b update_ml_manager_${BUILD_NUMBER}
            git status
            git add \$(pwd)/kubernetes/charts/splice/values.yaml
            git commit -m 'Update image tag to ${BUILD_NUMBER}'
            git push 
            """

        }
    }
    

    } catch (any) {
        // if there was an exception thrown, the build failed
        currentBuild.result = "FAILED"
        throw any

    } finally {

        // success or failure, always send notifications
        notifyBuild(currentBuild.result)
    }
}

def notifyBuild(String buildStatus = 'STARTED') {
    // Build status of null means successful.
    buildStatus =  buildStatus ?: 'SUCCESSFUL'
    // Override default values based on build status.
    if (buildStatus == 'STARTED' || buildStatus == 'INPUT') {
        color = 'YELLOW'
        colorCode = '#FFFF00'
    } else if (buildStatus == 'CREATING' || buildStatus == 'DESTROYING'){
        color = 'BLUE'
        colorCode = '#0000FF'
    } else if (buildStatus == 'SUCCESSFUL') {
        color = 'GREEN'
        colorCode = '#00FF00'
    } else if (buildStatus == 'FAILED'){
        color = 'RED'
        colorCode = '#FF0000'
    } else {
        echo "End of pipeline"
    }
}