"""
Service for creating/updating/deleting YAML Kubernetes Manifests
from python.

Based on code from https://stackoverflow.com/a/54964944/4501002 by "Ichthyo", which was licensed under Apache 2.0
"""

import re
from typing import BinaryIO, Callable, Union

import yaml
from kubernetes import client as k8s
from kubernetes import config

from shared.logger.logging_config import logger


class KubernetesAPIService:
    """
    Service for interacting with the Kubernetes API from inside of
    a pod
    """
    OPS_ENABLED = True

    logger.info("Configuring Kubernetes API Service to use in-cluster Service Account")
    try:
        config.load_incluster_config()  # load config from service account inside cluster
    except config.config_exception.ConfigException:
        logger.error(
            "Cannot load K8s config... KUBERNETES DEPLOYMENT WILL *NOT* RUN SUCCESSFULLY FROM THIS ENVIRONMENT."
        )
        OPS_ENABLED = False

    @staticmethod
    def _operation_from_yaml(data: Union[BinaryIO, str], callback: Callable, client=None, **kwargs):
        """
        Run a specified callback operation on given yaml data (may be multiple objects)
        :param data: either a string or an opened input stream with a
                        YAML formatted spec, as you'd use for `kubectl apply -f`
        :param callback: the callback function that receives the parsed obj
        :param client: (optional) preconfigured client environment to use for invocation
        :param kwargs: (optional) further arguments to pass to the callback call
        :return: response object from Kubernetes API call
        """
        logger.info("Creating Manifests from Input YAML...")
        if not KubernetesAPIService.OPS_ENABLED:
            raise Exception("Kubernetes Operations are disabled outside of a K8s cluster.")

        for manifest in yaml.load_all(data):
            callback(manifest, client, **kwargs)

    @staticmethod
    def add_from_yaml(*, data: Union[BinaryIO, str], client=None, **kwargs):
        """
        Add one/multiple objects to Kubernetes (Patch if already exists)
        :param data: input yaml (one or multiple objects)
        :param client: (optional) preconfigured client environment to use for invocation
        :param kwargs: (optional) further arguments to pass to the callback call
        :return: response object from Kubernetes API call
        """
        KubernetesAPIService._operation_from_yaml(data=data, client=client, callback=KubernetesAPIService.add_object,
                                                  **kwargs)

    @staticmethod
    def patch_from_yaml(*, data: Union[BinaryIO, str], client=None, **kwargs):
        """
        Patch one/multiple objects to Kubernetes
        :param data: input yaml (one or multiple objects)
        :param client: (optional) preconfigured client environment to use for invocation
        :param kwargs: (optional) further arguments to pass to the callback call
        :return: response object from Kubernetes API call
        """
        KubernetesAPIService._operation_from_yaml(data=data, client=client, callback=KubernetesAPIService.patch_object,
                                                  **kwargs)

    @staticmethod
    def delete_from_yaml(*, data: Union[BinaryIO, str], client=None, **kwargs):
        """
        Delete one/multiple objects from Kubernetes
        :param data: input yaml (one or multiple objects)
        :param client: (optional) preconfigured client environment to use for invocation
        :param kwargs: (optional) further arguments to pass to the callback call
        :return: response object from Kubernetes API call
        """
        KubernetesAPIService._operation_from_yaml(data=data, client=client, callback=KubernetesAPIService.delete_object,
                                                  **kwargs)

    @staticmethod
    def replace_from_yaml(*, data: Union[BinaryIO, str], client=None, **kwargs):
        """
        Replace one/multiple Kubernetes objects by deleting them if they exist,
        and recreating them
        :param data: input yaml (one or multiple objects)
        :param client: (optional) preconfigured client environment to use for invocation
        :param kwargs: (optional) further arguments to pass to the callback call
        :return: response object from Kubernetes API call
        """
        KubernetesAPIService.delete_from_yaml(data=data, client=client, **kwargs)
        KubernetesAPIService.add_from_yaml(data=data, client=client, **kwargs)

    @staticmethod
    def locate_object_api(manifest, client=None):
        """
        Investigate the object spec and lookup the corresponding API object
        :param manifest: the obj object to look up the API endpoint for
        :param client: (optional) preconfigured client environment to use for invocation
        :return: a client instance wired to the appropriate API
        """
        logger.debug("Locating K8s API Endpoint for obj")
        grp, _, version = manifest['apiVersion'].partition('/')
        if version == '':
            version = grp
            grp = 'core'
        # Strip 'k8s.io', camel-case-join dot separated parts.
        grp = ''.join(part.capitalize() for part in grp.rsplit('.k8s.io', 1)[0].split('.'))
        return getattr(k8s, f'{grp}{version.capitalize()}Api')(client)

    @staticmethod
    def get_name(manifest):
        """
        Readable description for obj name
        :param manifest: obj object
        :return: readable name
        """
        return f"{manifest['kind']} '{manifest['metadata']['name']}'"

    @staticmethod
    def invoke_api(api, action, manifest, **args):
        """
        Find a suitable function and perform the actual API invocation.
        :param api: client object for the invocation, wired to correct API version
        :param action: either 'create' (to inject a new object) or 'replace','patch','delete'
        :param manifest: the full object spec to be passed into the API invocation
        :param args: (optional) extraneous arguments to pass
        :return: response object from Kubernetes API call
        """
        # transform ActionType from Yaml into action_type for swagger API
        kind = KubernetesAPIService._camel_to_snake(manifest['kind'])
        # determine namespace to place the object in, supply default
        try:
            namespace = manifest['metadata']['namespace']
        except KeyError:
            logger.debug("Unable to locate namespace parameter... setting to default")
            namespace = 'default'

        function_name = f'{action}_{kind}'
        if hasattr(api, function_name):
            # namespace agnostic API
            function = getattr(api, function_name)
        else:
            function_name = f'{action}_namespaced_{kind}'
            function = getattr(api, function_name)
            args['namespace'] = namespace

        if 'create' not in function_name:
            args['name'] = manifest['metadata']['name']

        if 'delete' in function_name:
            from kubernetes.client.models.v1_delete_options import \
                V1DeleteOptions
            manifest = V1DeleteOptions()

        return function(body=manifest, **args)

    @staticmethod
    def add_object(obj, client=None, **kwargs):
        """
        Invoke the K8s API to create or replace a kubernetes object.
        The first attempt is to create(insert) this object; when this is rejected because
        of an existing object with same name, we attempt to patch this existing object.

        :param obj: complete object specification, including API version and metadata.
        :param client: (optional) preconfigured client environment to use for invocation
        :param kwargs: (optional) further arguments to pass to the create/replace call
        :return: response object from Kubernetes API call
        """
        api = KubernetesAPIService.locate_object_api(obj, client=client)

        try:
            res = KubernetesAPIService.invoke_api(api, 'create', obj, **kwargs)
            logger.debug(f'K8s: {KubernetesAPIService.get_name(obj)} created -> uid={res.metadata.uid}')
        except k8s.rest.ApiException as api_exc:
            logger.exception("Failed to create object...")
            if api_exc.reason != 'Conflict':
                raise
            # attempt to patch the existing object
            logger.warning("Attempting to patch...")
            res = KubernetesAPIService.invoke_api(api, 'patch', obj, **kwargs)
            logger.debug(f'K8s: {KubernetesAPIService.get_name(obj)} Patched -> uid={res.metadata.uid}')
        return res

    @staticmethod
    def patch_object(obj, client=None, **kwargs):
        """
        Invoke the Kubernetes API to patch an existing object with
        new information

        :param obj: Kubernetes Manifest
        :param client: (optional) preconfigured client environment to use for invocation
        :param kwargs: (optional) further arguments to pass to the patch call
        :return: response object from Kubernetes API call
        """
        api = KubernetesAPIService.locate_object_api(obj, client=client)
        try:
            res = KubernetesAPIService.invoke_api(api, 'patch', obj, **kwargs)
            logger.debug(f'K8s: {KubernetesAPIService.get_name(obj)} patched -> uid={res.metadata.uid}')
            return res
        except client.rest.ApiException:
            logger.exception("Unable to Patch Entity")
            raise

    @staticmethod
    def delete_object(obj, client=None, **kwargs):
        """
        Invoke the Kubernetes API to delete an existing object

        :param obj: Kubernetes Manifest
        :param client: (optional) preconfigured client environment to use for invocation
        :param kwargs: (optional) further arguments to pass to the patch call
        :return: response object from Kubernetes API call
        """
        api = KubernetesAPIService.locate_object_api(obj, client)
        try:
            res = KubernetesAPIService.invoke_api(api, 'delete', obj, **kwargs)
            logger.debug(
                f'K8s: {KubernetesAPIService.get_name(obj)} deleted -> uid was={res.details and res.details.uid or "?"}'
            )
            return True
        except k8s.rest.ApiException as api_exc:
            if api_exc.reason == 'Not Found':
                logger.warning(f'K8s: {KubernetesAPIService.get_name(obj)} does not exist (anymore).')
                return False
            logger.exception("Unable to delete object")
            raise

    @staticmethod
    def _camel_to_snake(string):
        """
        Utility function to convert input from camel case
        to snake case for the Kubernetes API
        :param string: input to convert to snake case
        :return: converted input
        """
        string = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', string)
        string = re.sub('([a-z0-9])([A-Z])', r'\1_\2', string).lower()
        return string
