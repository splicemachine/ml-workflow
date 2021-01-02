"""
Contains code pertaining to retraining deployment
"""
from os import environ as env_vars
from subprocess import check_output
from tempfile import NamedTemporaryFile

from shared.models.enums import RecurringJobStatuses
from shared.models.splice_models import RecurringJob
from shared.services.kubernetes_api import KubernetesAPIService
from yaml import dump as dump_yaml

from .base_deployment_handler import BaseDeploymentHandler

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Ben Epstein"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"


class RetrainingDeploymentHandler(BaseDeploymentHandler):
    """
    Deployment handler for retraining
    """
    DEFAULT_RETRIEVER_TAG = '0.0.13'
    DEFAULT_RETRAINER_TAG = '0.0.1'

    def __init__(self, task_id: int) -> None:
        """
        Initialize retraining handler constructor
        :param task_id: task id to process
        """
        BaseDeploymentHandler.__init__(self, task_id, logging_buffer_size=5)

    def _build_template_parameters(self):
        """
        Build the Values parameters in JSON format
        :return:
        """
        payload = self.task.parsed_payload
        return {
            'k8s': {'namespace': env_vars['NAMESPACE'], 'ownerPod': env_vars['POD_NAME'],
                    'ownerUID': env_vars['POD_UID'], 'mlflowUrl': env_vars['MLFLOW_URL']},
            'entity': {'entityId': payload['run_id'], 'retraining': 'yes', 'jobId': str(self.task_id),
                       'name': payload['name'].replace("_", "-"), 'condaEnv': payload['conda_artifact'],
                       'schedule': payload['cron_exp']},
            'db': {'user': env_vars['DB_USER'], 'password': env_vars['DB_PASSWORD'], 'host': env_vars['DB_HOST'],
                   'jdbcUrl': f"jdbc:splice://{env_vars['DB_HOST']}:1527/splicedb;user={env_vars['DB_USER']};"
                              f"password={env_vars['DB_PASSWORD']};impersonate={self.task.user}"
                   },
            'versions': {'retriever': env_vars.get('RETRIEVER_IMAGE_TAG',
                                                   RetrainingDeploymentHandler.DEFAULT_RETRIEVER_TAG),
                         'retrainer': env_vars.get('RETRAINER_IMAGE_TAG',
                                                   RetrainingDeploymentHandler.DEFAULT_RETRAINER_TAG)},
        }

    def _add_scheduled_job(self):
        """
        Add the retraining Job to the scheduled jobs table if one doesn't already exist and isn't being recreated by
        Bobby
        """
        # Special flag only sent by Bobby on recreation. If Bobby is sending this in directly, if means the database
        # was just paused and resumed, so we don't want to add a new record to the recurring_jobs table. Just submit the
        # CronJob to Kubernetes
        if self.task.parsed_payload.get('skip_validation'):
            pass
        else:
            job_name = self.task.parsed_payload['name']
            entity_id = self.task.parsed_payload['run_id']
            current_recurring_job = self.Session.query(RecurringJob) \
                .filter(RecurringJob.entity_id == entity_id) \
                .filter(RecurringJob.name == job_name).one_or_none()
            # If the job already exists, fail (user submitted a job with an already existing job name)
            if current_recurring_job:
                self.logger.exception(f'Recurring job with name {job_name} for run {entity_id} already exists. '
                                      f'Recurring Job names must be unique for a given run/feature set/user')
                raise

            recurring_job = RecurringJob(name=job_name,
                                         status=RecurringJobStatuses.active,
                                         job_id=self.task_id,
                                         user=self.task.user,
                                         entity_id=entity_id)
            self.Session.merge(recurring_job)

    def _create_kubernetes_manifests(self):
        """
        Create a values file in /tmp to render the helm template
        :return: manifests as a string
        """
        with NamedTemporaryFile(suffix='.yaml', mode='w') as tmpf:
            self.logger.info("Creating Retraining helm chart values to render", send_db=True)
            dump_yaml(self._build_template_parameters(), tmpf.file)
            self.logger.info("Rendering template...", send_db=True)

            rendered_templates = check_output(['helm', 'template',
                                               f"{env_vars['SRC_HOME']}/configuration/k8s_retraining_helm",
                                               "--values", tmpf.name, "--debug"])

            KubernetesAPIService.add_from_yaml(data=rendered_templates)

    def execute(self) -> None:
        """
        Deploy Retraining Job
        :return:
        """
        steps = [self._add_scheduled_job, self._create_kubernetes_manifests]
        for execute_step in steps:
            execute_step()
