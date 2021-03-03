"""
Contains handler and functions
pertaining to Kubernetes Model Deployment
"""
from os import environ as env_vars
from subprocess import check_output
from tempfile import NamedTemporaryFile

from yaml import dump as dump_yaml

from shared.services.kubernetes_api import KubernetesAPIService

from .base_deployment_handler import BaseDeploymentHandler


class KubernetesUndeploymentHandler(BaseDeploymentHandler):
    """
    Handler for removing deployment of model from Kubernetes
    """

    def __init__(self, task_id: int):
        """
        Initialize Base Handler
        constructor (set instance variables
        etc.)

        :param task_id: (int) Id of job to process
        """
        BaseDeploymentHandler.__init__(self, task_id)

    def _build_template_parameters(self):
        """
        Build the Values parameters in json format. Since we are deleting the deployment,
        mock out all other values so we don't need to create new charts. The deployment name is always
        model-<run_id> so that's all we need to fill in here. This will pass the helm template

        """
        payload = self.task.parsed_payload
        return {
            'baseReplicas': 'None',
            'k8s': {'namespace': env_vars['NAMESPACE'], 'ownerPod': env_vars['POD_NAME'],
                    'ownerUID': env_vars['POD_UID']},
            'model': {'runId': payload['run_id'], 'name': 'None', 'namespace': env_vars['NAMESPACE']},
            'db': {'user': env_vars['DB_USER'], 'password': env_vars['DB_PASSWORD'],
                   'host': env_vars['DB_HOST']},
            'versions': {'retriever': 'None', 'server': 'None'},
            'serving': {'gunicornWorkers': 'None', 'disableNginx': "false",
                        'exposePort': 'None'},
            'resourceRequests': {'enabled': "false",
                                 'cpu_request': 'None', 'memory_request': 'None'},
            'resourceLimits': {'enabled': "false",
                               'cpu_limit': 'None', 'memory_limit': 'None'},
            'autoscaling': {'enabled': "false", 'maxReplicas': 'None',
                            'targetCPULoad': 'None'}
        }

    def _create_kubernetes_manifests(self):
        """
        Create a values file in /tmp to render the helm template so we can delete the chart (deployment)
        :return: manifests as a string
        """
        with NamedTemporaryFile(suffix='.yaml', mode='w') as tf:
            self.logger.info("Creating Helm Values File to Parametrize Kubernetes Manifests...", send_db=True)
            dump_yaml(self._build_template_parameters(), tf.file)
            self.logger.info("Rendering template...", send_db=True)
            rendered_templates = check_output(['helm', 'template',
                                               f"{env_vars['SRC_HOME']}/configuration/k8s_serving_helm",
                                               "--values", tf.name])

            KubernetesAPIService.delete_from_yaml(data=rendered_templates)


    def execute(self):
        """
        Deploy Job to Kubernetes
        :return:
        """
        steps: tuple = (
            self._create_kubernetes_manifests,
        )

        for execute_step in steps:
            execute_step()
