from .aws_deployment_handler import SageMakerDeploymentHandler
from .azure_deployment_handler import AzureDeploymentHandler
from .database_deployment_handler import DatabaseDeploymentHandler
from .database_undeployment_handler import DatabaseUndeploymentHandler
from .kubernetes_deployment_handler import KubernetesDeploymentHandler
from .kubernetes_undeployment_handler import KubernetesUndeploymentHandler

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Murray Brown", "Monte Zweben", "Ben Epstein"]

__license__: str = "Commercial"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"
__status__: str = "Quality Assurance (QA)"
