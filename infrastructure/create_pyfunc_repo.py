import sys
import os
import boto3
import docker
import json
import base64
import logging
from subprocess import Popen

# define vars and functions
REPO_NAME = 'mlflow-pyfunc'
IMAGE_TAG = '1.1.0'
ACCESS_KEY = os.environ['AWS_ACCESS_KEY_ID']
SECRET_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

def _repository_exists(client, repo_name):
    response = client.describe_repositories()
    for repository in response['repositories']:
        if repository['repositoryName'] == repo_name:
            return True
    return False

def _image_tag_exists(client, repo_name, image_tag):
    images = client.list_images(repositoryName=repo_name)
    for i in images['imageIds']:
        try:
            if(i['imageTag'] == image_tag):
                return True
        except:
            continue
    return False

def _get_uri(client, repo_name):
    response = client.describe_repositories()
    for repository in response['repositories']:
        if repository['repositoryName'] == repo_name:
            return repository['repositoryUri']

def ecr_docker_login(client, docker_client):
    token = client.get_authorization_token()
    username, password = base64.b64decode(
        token['authorizationData'][0]['authorizationToken']).decode().split(':')
    registry = token['authorizationData'][0]['proxyEndpoint']
    docker_client.login(username, password, registry=registry)
    auth_config = {'username': username, 'password': password}
    return auth_config

def main():
    #start the docker deamon
    Popen(['dockerd'])
 
    full_image = 'splicemachine/{}:{}'.format(REPO_NAME, IMAGE_TAG)

    # read config file
    js = open('infrastructure/config.json').read()
    conf = json.loads(js)
    # create the docker_client
    # docker_client = docker.from_env() (for running outside Jenkins)
    docker_client = docker.APIClient(base_url='unix://var/run/docker.sock')

    # us-west-1, us-west-2 etc
    for reg in conf['regions']:
        logging.warning('Creating ECR client for {}'.format(reg))
        # recreate the client for each region... will need new creds for prod/qa?
        client = boto3.client('ecr',
                              region_name=reg,
                              aws_access_key_id=ACCESS_KEY,
                              aws_secret_access_key=SECRET_KEY
                              )
        logging.warning('Checking if repo exists...')
        if not _repository_exists(client, REPO_NAME):
            logging.warning(
                'No repo exists... Creating new ECR Repo: ' + REPO_NAME)
            client.create_repository(repositoryName=REPO_NAME)
        logging.warning('Repo exists in {}, skipping creation'.format(reg))
        logging.warning('Checking if image exists...')
        if not _image_tag_exists(client, REPO_NAME, IMAGE_TAG):
            logging.warning(
                'No image exists... Pushing image : {}:{}'.format(REPO_NAME, IMAGE_TAG))
            # get the docker image
            docker_client.pull(full_image)
            # get uri from ECR and tag docker image
            uri = _get_uri(client, REPO_NAME)
            # tag the image
            docker_client.tag(full_image, uri, tag=IMAGE_TAG)
            # login to ecr from docker client
            auth_config = ecr_docker_login(client, docker_client)
            # push image
            docker_client.push(uri, tag=IMAGE_TAG, auth_config=auth_config)
        logging.warning(
            'Image exists for region {}, skipping push'.format(reg))


if __name__ == "__main__":
    main()
