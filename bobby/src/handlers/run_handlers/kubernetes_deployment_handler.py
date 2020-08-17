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


class KubernetesDeploymentHandler(BaseDeploymentHandler):
    """
    Handler for processing deployment to Kubernetes
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
        Build the Values parameters in json format
        :return:
        """
        payload = self.task.parsed_payload
        return {
            'baseReplicas': payload['base_replicas'],
            'k8s': {'namespace': env_vars['NAMESPACE'], 'ownerPod': env_vars['POD_NAME'],
                    'ownerUID': env_vars['POD_UID']},
            'model': {'runId': payload['run_id'], 'name': self.model_dir, 'namespace': env_vars['NAMESPACE']},
            'db': {'user': env_vars['DB_USER'], 'password': env_vars['DB_PASSWORD'],
                   'host': env_vars['DB_HOST']},
            'versions': {'retriever': env_vars['RETRIEVER_IMAGE_TAG'], 'server': env_vars['SERVER_IMAGE_TAG']},
            'serving': {'gunicornWorkers': payload['gunicorn_workers'], 'disableNginx': payload['disable_nginx'],
                        'exposePort': payload['service_port']},
            'resourceRequests': {'enabled': payload['resource_requests_enabled'],
                                 'cpu_request': payload['cpu_request'], 'memory_request': payload['memory_request']},
            'resourceLimits': {'enabled': payload['resource_limits_enabled'],
                               'cpu_limit': payload['cpu_limit'], 'memory_limit': payload['memory_limit']},
            'autoscaling': {'enabled': payload['autoscaling_enabled'], 'maxReplicas': payload['max_replicas'],
                            'targetCPULoad': payload['target_cpu_utilization']}
        }

    def _create_kubernetes_manifests(self):
        """
        Create a values file in /tmp to render the helm template
        :return: manifests as a string
        """
        with NamedTemporaryFile(suffix='.yaml') as tf:
            self.logger.info("Creating Helm Values File to Parametrize Kubernetes Manifests...", send_db=True)
            dump_yaml(self._build_template_parameters(), tf)
            self.logger.info("Rendering template...", send_db=True)
            rendered_templates = check_output(['helm', 'template',
                                               f"{env_vars['WORKER_HOME']}/configuration/k8s_serving_helm",
                                               "--values", tf.name])

            KubernetesAPIService.add_from_yaml(data=rendered_templates)

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
