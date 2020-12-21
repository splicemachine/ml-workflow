"""
Contains code pertaining to retraining deployment
"""
from os import environ as env_vars
from subprocess import check_output
from tempfile import NamedTemporaryFile

from shared.services.kubernetes_api import KubernetesAPIService
from yaml import dump as dump_yaml

from shared.shared.models.enums import RecurringJobStatuses
from shared.shared.models.splice_models import RecurringJob
from .base_deployment_handler import BaseDeploymentHandler

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"


class RetrainingDeploymentHandler(BaseDeploymentHandler):
    """
    Deployment handler for retraining
    """

    def __init__(self, task_id: int) -> None:
        """
        Initialize retraining handler constructor
        :param task_id: task id to process
        """
        BaseDeploymentHandler.__init__(self, task_id)

    def _build_template_parameters(self):
        """
        Build the Values parameters in JSON format
        :return:
        """
        payload = self.task.parsed_payload
        return {
            'k8s': {'namespace': env_vars['NAMESPACE'], 'ownerPod': env_vars['POD_NAME'],
                    'ownerUID': env_vars['POD_UID'], 'name': payload['name']},
            'model': {'runId': payload['run_id'], 'retraining': 'yes', 'namespace': env_vars['NAMESPACE'],
                      'condaEnv': payload['conda_artifact'], 'schedule': payload['schedule']},
            'db': {'user': env_vars['DB_USER'], 'password': env_vars['DB_PASSWORD'], 'host': env_vars['DB_HOST'],
                   'jdbc_url': f"jdbc:splice://{env_vars['DB_HOST']}:1527/splicedb;user={env_vars['DB_USER']};password={env_vars['DB_PASSWORD']}"}
        }

    def _add_scheduled_job(self):
        """
        Add the retraining Job to the scheduled jobs table
        """
        current_recurring_job = self.Session.query(RecurringJob).where
        recurring_job = RecurringJob(name=self.task.parsed_payload['name'],
                                     status=RecurringJobStatuses.active,
                                     job_id=self.task_id)
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
                                               f"{env_vars['SRC_HOME']}/configuration/k8s_retraining-helm",
                                               "--values", tmpf.name])

            KubernetesAPIService.add_from_yaml(data=rendered_templates)

    def execute(self) -> None:
        """
        Deploy Retraining Job
        :return:
        """
        self._create_kubernetes_manifests()
