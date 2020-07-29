import base64
import json
import logging
import os
import sys
from subprocess import Popen

import boto3
import docker

# define vars and functions
REPO_NAME = 'mlflow-pyfunc'
IMAGE_TAG = '1.1.0'
ACCESS_KEY = os.environ['AWS_ACCESS_KEY_ID']
SECRET_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
LOGGER = logging.getLogger(__name__)

def _repository_exists(client, repo_name):
    """
    Checks if a repository exists
    :param client: boto3.client The ECR client 
    :param repo_name: str The name of the repository to check
    """
    response = client.describe_repositories()
    #will either return a list of dicts or empty dict
    for repository in response.get('repositories',{}):
        #if empty dict is returned, loop skipped and False returned
        if repository.get('repositoryName') == repo_name:
            return True
    return False

def _image_tag_exists(client, repo_name, image_tag):
    """
    Checks if a image tag exists
    :param client: boto3.client The ECR client 
    :param repo_name: str The name of the repository to check
    :param image_tag: str The image tag to check
    """
    images = client.list_images(repositoryName=repo_name)
    #return empty dictionary if key not found
    for i in images.get('imageIds',{}):
        #if empty dict is returned, loop skipped and False returned
        if i.get('imageTag') == image_tag:
            return True
    return False

def _get_uri(client, repo_name):
    """
    Gets the uri from the ECR repo
    :param client: boto3.client The ECR client 
    :param repo_name: str The repository name to get the image URI
    """
    response = client.describe_repositories()
    for repository in response.get('repositories', {}):
        #if empty dict is returned, loop skipped and None returned
        if repository.get('repositoryName') == repo_name:
            return repository.get('repositoryUri')


def ecr_docker_login(client, docker_client):
    """
    Logs logs into Docker using the ECR client registry
    :param client: boto3.client the ECR client
    :param docker_client: docker.APIClient the docker client to log into
    """
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
        LOGGER.info('Creating ECR client for {}'.format(reg))
        # recreate the client for each region... will need new creds for prod/qa?
        client = boto3.client('ecr',
                              region_name=reg,
                              aws_access_key_id=ACCESS_KEY,
                              aws_secret_access_key=SECRET_KEY
                              )
        LOGGER.info('Checking if repo exists...')
        if not _repository_exists(client, REPO_NAME):
            LOGGER.info(
                'No repo exists... Creating new ECR Repo: ' + REPO_NAME)
            client.create_repository(repositoryName=REPO_NAME)
        LOGGER.info('Repo exists in {}, skipping creation'.format(reg))
        LOGGER.info('Checking if image exists...')
        if not _image_tag_exists(client, REPO_NAME, IMAGE_TAG):
            LOGGER.info(
                'No image exists... Pulling image from dockerhub and Pushing image to ECR: {}:{}'.format(REPO_NAME, IMAGE_TAG))
            # get the docker image
            docker_client.pull(full_image)
            # get uri from ECR and tag docker image
            uri = _get_uri(client, REPO_NAME)
            if not uri:
                LOGGER.error('URI not found for ECR image... Either ECR image does not exist in region or no URI associated with image')
                return 1
            # tag the image
            docker_client.tag(full_image, uri, tag=IMAGE_TAG)
            # login to ecr from docker client
            auth_config = ecr_docker_login(client, docker_client)
            # push image
            docker_client.push(uri, tag=IMAGE_TAG, auth_config=auth_config)
        LOGGER.info(
            'Image exists for region {}, skipping push'.format(reg))


if __name__ == "__main__":
    main()
