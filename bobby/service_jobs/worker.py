import os
import subprocess
import time
import traceback

import yaml
import boto3
from pyspark import SparkConf
import mlflow
import mlflow.sagemaker

from splicemachine_queue import SpliceMachineQueue

import logging
import sys
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


"""
This module contains a worker that reads a Splice Machine Queue and redirects it to the
correct handler. This module allows for jobs to be handled asynchronously, rather than waiting
for a response to be returned by the MLFlow RESTful API.
Runtime for deployment: ~ 4.5 mins

"""

# TODO: Add Scheduling and Retraining to this file. Also, deploy to SageMaker if thresh is passed

__author__ = "Splice Machine, Inc."
__copyright__ = "Copyright 2018, Splice Machine Inc. All Rights Reserved"
__credits__ = ["Amrit Baveja", "Murray Brown", "Monte Zweben", "Ben Epstein"]

__license__ = "Commerical"
__version__ = "2.0"
__maintainer__ = "Amrit Baveja"
__email__ = "abaveja@splicemachine.com"
__status__ = "Quality Assurance (QA)"

# logging.basicConfig(format='%(message)s')
# logging.basicConfig(level=logging.INFO,
#                     format="%(asctime)s - %(name)s (%(lineno)s) - %(levelname)s: %(message)s",
#                     datefmt='%Y.%m.%d %H:%M:%S')
# logger = logging.getLogger('worker')
# logger.setLevel(logging.DEBUG)

HANDLERS = ['deploy', 'retrain']


class BaseHandler(object):
    """
    Base Class for all Handlers
    """

    def __init__(self, queue, task):
        self.queue = queue
        self.task = task

    @staticmethod
    def _format_python_string_as_html(string):
        """Turn a traceback into HTML so that it can be rendered in the viewer
        :param string: code to format in an html codeblock

        """
        replacements = {
            '\n': '<br>',
            "'": ""
        }
        for key, value in replacements.items():
            string = string.replace(key, value)
        return '<br><code>' + string + '</code>'


class StartServiceHandler(BaseHandler):
    def __init__(self, queue, task):
        BaseHandler.__init__(self, queue, task)
        self.start_service_handler(task)

    def start_service_handler(self, task):
        """
        Enable a specific service
        :param task: task from json
        :return: True on success, False on exception
        """
        logging.warning("start_service_handler", task)
        service_to_start = task.payload['service']
        if self.queue.enable_service(service_to_start):
            self.queue.upstat(task.job_id, 'UPDATED')
            return True
        else:
            self.queue.upstat(task.job_id, 'FAILED')
            return False


class StopServiceHandler(BaseHandler):
    def __init__(self, queue, task):
        BaseHandler.__init__(self, queue, task)
        self.stop_service_handler(task)

    def stop_service_handler(self, task):
        """
        Disable a specific service
        :param task: task from json
        :return: True on success, False on exception
        """
        service_to_stop = task.payload['service']
        if self.queue.disable_service(service_to_stop):
            self.queue.upstat(task.job_id, 'UPDATED')
            return True
        else:
            self.queue.upstat(task.job_id, 'FAILED')
            return False


class StartScheduleHandler(BaseHandler):
    """
    Handler for starting the scheduling of a job
    """

    def __init__(self, queue, task):
        BaseHandler.__init__(self, queue, task)
        self.start_scheduler_handler(task)

    def start_scheduler_handler(self, task):
        """
        Start a metrenome app that will resubmit task every task.payload['interval'] minutes
        :param task: task from json
        :return: NotImplementedError (until implemented)
        """

        raise NotImplementedError()


class StopScheduleHandler(BaseHandler):
    """
    Handler for stopping the scheduling of a job
    """

    def __init__(self, queue, task):
        BaseHandler.__init__(self, queue, task)

    def stop_scheduler_handler(self, task):
        raise NotImplementedError()


class DeploymentHandler(BaseHandler):
    """
    Handler for handling deployment jobs
    """

    def __init__(self, queue, task, sleep_interval=3):
        BaseHandler.__init__(self, queue, task)
        self.sleep_interval = sleep_interval
        self.root_path = '/bob/tmp/'
        self.client = boto3.client('ecr', region_name=task.payload['sagemaker_region'], verify=False)
        self.deploy_handler(task)

    def __repository_exists(self, repo_name):
        """
        Return whether or not a repository exists in ECR
        :param repo_name: the repository to check for the existance of
        """
        response = self.client.describe_repositories()
        for repository in response['repositories']:
            if repository['repositoryName'] == repo_name:
                return True
        return False

    def create_ecr_repo(self, repo_name):
        """
        Create an ECR Repo if it doesn't exist
        :param repo_name: the repository to check/create
        """
        if not self.__repository_exists(image_name):
            self.client.create_repository(
                    repositoryName=repo_name
                )
            

    def conda_setup(self, task_id, model_path):
        """Setup conda environment so that SageMaker will not throw any errors
        :param task_id: the task_id given from a metadata hash (in a job)
        :param model_path: the model artifact location under the specified run
        """

        self.queue.upinfo(
            task_id, 'Configuring Conda Environment')  # Update info
        logging.warning('doing conda')

        # Conda YAML File Contents for SageMaker
        conda_yaml_contents = \
            """
            name: pyspark_conda
            channels:
              - defaults
            dependencies:
              - python=3.6
              - pandas=0.22.0
              - pip:
                - pyspark==2.2.2
                - mlflow==0.8.0
            """
        with open(model_path + '/conda.yaml',
                  'w') as conda_file:  # Write conda yaml to a file under the model path
            logging.warning('writing pkl to: ' + model_path + '/conda.yaml')
            logging.warning(conda_yaml_contents)
            conda_file.write(conda_yaml_contents)  # write

        with open(model_path + '/MLmodel') as mlmodel_yaml:  # Open model metadata file
            yf = yaml.load(mlmodel_yaml)  # load it as yaml
            logging.warning(yf)
            yf['flavors']['python_function'][
                'env'] = 'conda.yaml'  # tell docker to use the conda environment on AWS
            logging.warning(yf)
            logging.warning('writing mlmodel' + 'to ' + model_path + '/MLmodel')

        with open(model_path + '/MLmodel', 'w') as ml_write_yml:
            yaml.dump(yf, ml_write_yml, default_flow_style=False)  # write it to a file

    @staticmethod
    def change_s3_creds():
        splice_access_key_id_aws = os.environ.get('SPLICE_AWS_ACCESS_KEY_ID')
        splice_secret_key_aws = os.environ.get('SPLICE_AWS_SECRET_ACCESS_KEY')

        if not (splice_access_key_id_aws or splice_secret_key_aws):
            raise Exception("No Splice AWS Credentials Found! ERROR!")

        os.environ['AWS_ACCESS_KEY_ID'] = splice_access_key_id_aws
        os.environ['AWS_SECRET_ACCESS_KEY'] = splice_secret_key_aws

    def download_current_s3_state(self, task_id, run_id, experiment_id):
        """
        Download the current S3 MLFlow bucket
        :param task_id: job_id from task
        :return:
        """

        logging.warning("task_id: " + task_id)
        logging.warning("run_id: " + run_id)
        logging.warning("experiment_id: " + experiment_id)

        self.queue.upinfo(task_id, 'Downloading Artifact from S3')
        artifact_uri = '{s3_bucket}/artifacts/{experiment}/{run_id}/'.format(
            s3_bucket=os.environ.get('S3_BUCKET_NAME'),
            run_id=run_id,
            experiment=experiment_id
        )

        logging.warning("artifact_uri: " + artifact_uri)
        logging.warning("path: " + self.root_path + "model/")

        subprocess.check_call([
            'aws', 's3', 'sync', artifact_uri, self.root_path + "model/"
        ])

        logging.warning("returning this path from download_current_s3_state " +  self.root_path + "model")
        return self.root_path + "model"

    def deploy_handler(self, task):
        """
        Deploy a specified job to SageMaker
        :param task: task to deploy
        :return:
        """
        try:
            if self.queue.is_service_allowed(task.handler):
                os.environ['AWS_DEFAULT_REGION'] = task.payload['sagemaker_region']

                logging.warning(task.payload)
                path = self.download_current_s3_state(
                    task.job_id,
                    task.payload['run_id'],
                    task.payload['experiment_id']
                )  # Download the current S3 State
                self.change_s3_creds() # switch to splice aws creds
                time.sleep(self.sleep_interval)  # Pause for 3 secs
                self.conda_setup(task.job_id,
                                 path + '/artifacts/' + task.payload[
                                     'postfix'])  # Setup conda environment
                self.create_ecr_repo()
                self.build_and_push_image(task.job_id)  # Push image to ECR

                time.sleep(self.sleep_interval)
                self.deploy_model_to_sagemaker(
                    task.job_id,
                    task.payload,
                    path + '/artifacts/' + task.payload['postfix']
                )  # Deploy model to SageMaker

                self.queue.dequeue(task.job_id)  # Dequeue task from queue (Finish the job)
                self.queue.upinfo(task.job_id, 'Pushed Model to SageMaker! Success.')  # update info
            else:
                self.queue.upinfo(task.job_id, 'Failure!<br>' + 'Deployment is disabled!')
                self.queue.dequeue(task.job_id, True)


        except:
            stack_trace = self._format_python_string_as_html(traceback.format_exc())
            print(stack_trace)
            self.queue.upinfo(task.job_id, 'Failure!<br>' + stack_trace)
            self.queue.dequeue(task.job_id, True)

    def build_and_push_image(self, task_id):
        """
        Push and build MLFlow docker image to ECR
        :param task_id: the job id calculated by hashing task
        """
        self.queue.upinfo(task_id, 'Building and Pushing MLFlow Docker Container to ECR')
        mlflow.sagemaker.build_image(mlflow_home="/mlflow") # some weird java stuff........ is this screwing us up?
        mlflow.sagemaker.push_image_to_ecr()
        # MLFlow Maven is messed up so we will use another solution to get mlflow temporarily

    def deploy_model_to_sagemaker(self, task_id, payload, path):
        """
        Deploy a model to sagemaker using a specified iam role, app name, model path and region
        :param task_id: the job_id (calculated from metadata hash)
        :param payload: job payload e.g. app name, IAM role, sagemaker region
        """

        self.queue.upinfo(task_id, 'Deploying model to SageMaker')  # Update Information

        mlflow.sagemaker.deploy(payload['app_name'], path, execution_role_arn=payload['iam_role'],
             region_name=payload['sagemaker_region'], mode=payload['deployment_mode'],
             instance_type=payload['instance_type'], instance_count=int(payload['instance_count'])
        )

class RetrainHandler(BaseHandler):
    def __init__(self, task, queue):
        BaseHandler.__init__(self, queue, task)
        self.retrain_handler(task)

    def retrain_handler(self, task):
        """
        Retrain on parameters given by the JSON here. Retraining is still WIP until we get
        spark context to run outside of Zeppelin, so we need to finish this before we can push

        :param task: namedtuple containing task information
        :return:
        """
        try:
            raise NotImplementedError()
        except:
            stack_trace = self._format_python_string_as_html(traceback.format_exc())
            print(stack_trace)
            self.queue.upinfo(task.job_id, 'Failure!<br>' + stack_trace)
            self.queue.dequeue(task.job_id, True)


class Worker(object):
    """A worker that reads Splice Machine Queue"""

    def __init__(self):

        self.queue = SpliceMachineQueue()
        self.queue.set_unknown_services_to_enabled(HANDLERS)
        self.poll_interval = 17  # seconds to wait
        self.pause_interval = 3
        self.MAPPING = {
            "deploy": DeploymentHandler,
            "stop_service": StopServiceHandler,
            "start_service": StartServiceHandler,
            "retrain": RetrainHandler,
            "start_schedule": StartScheduleHandler,
            "stop_schedule": StopScheduleHandler
        }


    def loop(self):
        """Loop and wait for incoming requests"""
        while True:
            time.sleep(self.poll_interval)  # wait for next poll
            task = self.queue.service_job()
            if task:
                logging.warning(task)

                if task.handler in self.MAPPING:
                    handler = self.MAPPING[task.handler](self.queue, task)
                else:
                    logger.error("Task Not Understood!")


if __name__ == '__main__':
    w = Worker()
    w.loop()
