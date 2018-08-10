import logging
import os
import subprocess
import time
import traceback

import yaml
from splicemachine_queue import SpliceMachineQueue

"""
This module contains a worker that reads a Splice Machine Queue and redirects it to the
correct handfler. This module allows for jobs to be handled asynchronously, rather than waiting
for a response to be returned by the MLFlow Rest API.
Runtime for deployment: ~ 4.5 mins
"""

__author__ = "Splice Machine, Inc."
__copyright__ = "Copyright 2018, Splice Machine Inc. Some Rights Reserved"
__credits__ = ["Amrit Baveja", "Murray Brown", "Monte Zweben"]

__license__ = "Apache-2.0"
__version__ = "2.0"
__maintainer__ = "Amrit Baveja"
__email__ = "abaveja@splicemachine.com"
__status__ = "Quality Assurance (QA)"

logging.basicConfig()
logger = logging.getLogger('worker')
logger.setLevel(logging.DEBUG)


class Worker(object):
    """A worker that reads Splice Machine Queue"""

    def __init__(self):
        self.queue = SpliceMachineQueue()
        self.poll_interval = 5  # seconds

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

    def conda_setup(self, task_id, model_path):
        """Setup conda environment so that SageMaker will not throw any errors
        :param task_id: the task_id given from a metadata hash (in a job)
        :param model_path: the model artifact location under the specified run
        """

        self.queue.upinfo(
            task_id, 'Configuring Conda Environment')  # Update info
        logger.debug('doing conda')

        # Conda YAML File Contents for SageMaker
        conda_yaml_contents = \
            """
            name: pyspark_conda
            channels:
              - defaults
            dependencies:
              - python=3.6
              - numpy=1.14.3
              - pandas=0.22.0
              - scikit-learn=0.19.1
              - pyspark
              - h2o
              - pip:
                - mlflow==0.4.0
            """
        with open(model_path + '/conda.yaml',
                  'w') as conda_file:  # Write conda yaml to a file under the model path
            logger.debug('writing pkl to: ' + model_path + '/conda.yaml')
            logger.debug(conda_yaml_contents)
            conda_file.write(conda_yaml_contents)  # write

        with open(model_path + '/MLmodel') as mlmodel_yaml:  # Open model metadata file
            yf = yaml.load(mlmodel_yaml)  # load it as yaml
            logger.debug(yf)
            yf['flavors']['python_function'][
                'env'] = 'conda.yaml'  # tell docker to use the conda environment on AWS
            logger.debug(yf)
            logger.debug('writing mlmodel' + 'to ' + model_path + '/MLmodel')

        with open(model_path + '/MLmodel', 'w') as ml_write_yml:
            yaml.dump(yf, ml_write_yml, default_flow_style=False)  # write it to a file

    def download_current_s3_state(self, task_id):
        """Download the current S3 State (metadata bucket contents) ONCE

        Args:
          task_id: the current task_id for logging

        Returns:
          Exception if fails

        """
        self.queue.upinfo(task_id, 'Syncing Current MLFlow Data from S3')
        s3_bucket_name = os.environ.get('S3_BUCKET_NAME')
        logger.debug(s3_bucket_name)
        subprocess.call(['python',
                         '/bob/s3_sync.py',
                         'download',
                         '-b',
                         s3_bucket_name,
                         '-m',
                         '/mlruns',
                         '-i',
                         '/mlruns'])

    def deploy_handler(self, task):
        """Deploy a specified Job to SageMaker

        Args:
          task: return:

        Returns:

        """
        try:
            os.environ['AWS_DEFAULT_REGION'] = task.payload['sagemaker_region']

            logger.debug(task.payload)
            self.download_current_s3_state(task.job_id)  # Download the current S3 State

            time.sleep(3)  # Pause for 3 secs
            self.conda_setup(task.job_id, task.payload['ml_model_path'])  # Setup conda environment
            self.build_and_push_image(task.job_id)  # Push image to ECR

            time.sleep(3)
            self.deploy_model_to_sagemaker(task.job_id, task.payload)  # Deploy model to SageMaker

            self.queue.dequeue(task.job_id)  # Dequeue task from queue (Finish the job)
            self.queue.upinfo(task.job_id, 'Pushed Model to SageMaker! Success.')  # update info

        except Exception as e:
            logger.error('Failure: ' + str(e))
            stack_trace = Worker._format_python_string_as_html(traceback.format_exc())
            print(stack_trace)
            self.queue.upinfo(task.job_id, 'Failure!<br>' + stack_trace)
            self.queue.dequeue(task.job_id, True)

    def build_and_push_image(self, task_id):
        """Push and build MLFlow docker image to ECR

        Args:
          task_id: the job id calculated by hashing task

        Returns:
          Exception on failure

        """
        self.queue.upinfo(task_id, 'Building and Pushing MLFlow Docker Container to ECR')

        subprocess.call(
            ['mlflow', 'sagemaker', 'build-and-push-container'])  # Push and build container

    def deploy_model_to_sagemaker(self, task_id, payload):
        """Deploy a model to sagemaker using a specified iam role, app name, model path and region

        Args:
          task_id: the job_id (calculated from metadata hash)
          payload: job payload e.g. app name, IAM role, sagemaker region

        Returns:
          Exception on failure

        """

        self.queue.upinfo(task_id, 'Deploying model to SageMaker')  # Update Information

        subprocess.call(['mlflow',
                         'sagemaker',
                         'deploy',
                         '-a',
                         payload['app_name'],
                         '-m',
                         payload['ml_model_path'],
                         '-e',
                         payload['iam_role'],
                         '-r',
                         payload['sagemaker_region'],
                         '-t',
                         payload['instance_type'],
                         '-c',
                         payload['instance_count'],
                         '-md',
                         payload['deployment_mode']])

    def start_scheduler(self, task):
        pass

    def stop_scheduler(self, task):
        pass

    def retrain(self, task):
        pass

    def loop(self):
        """Loop and wait for incoming requests"""
        while True:
            time.sleep(self.poll_interval)  # wait for next poll
            task = self.queue.service_job()
            if task:
                logger.debug(task)
                if task.handler == 'deploy':
                    logger.debug('deploying...')
                    self.deploy_handler(task)
                elif task.handler == 'schedule_start':
                    self.start_scheduler(task)
                elif task.handler == 'schedule_stop':
                    self.stop_scheduler(task)
                elif task.handler == 'retrain':
                    self.retrain(task)


if __name__ == '__main__':
    w = Worker()
    w.loop()
