"""
Enums shared across various components
"""


class ModelStatuses:
    """
    Class containing names
    for In Database Model Deployments
    """
    deployed: str = 'DEPLOYED'
    deleted: str = 'DELETED'

    @staticmethod
    def get_valid() -> tuple:
        """
        Get valid model statuses
        :return: valid model statuses
        """
        return ModelStatuses.deleted, ModelStatuses.deployed


class JobStatuses:
    """
    Class containing names
    for valid Job statuses
    """
    pending: str = 'PENDING'
    success: str = 'SUCCESS'
    running: str = 'RUNNING'
    failure: str = 'FAILURE'

    @staticmethod
    def get_valid() -> tuple:
        """
        Get valid model statuses
        :return valid model statuses
        """
        return JobStatuses.pending, JobStatuses.success, JobStatuses.running, JobStatuses.failure


class RecurringJobStatuses:
    """
    Class containing names
    for recurring jobs
    """
    active: str = "ACTIVE"
    deleted: str = "DELETED"

    @staticmethod
    def get_valid() -> tuple:
        """
        Get valid recurring job statuses
        :return: valid statuses
        """
        return RecurringJobStatuses.active, RecurringJobStatuses.deleted


class FileExtensions:
    """
    Class containing names for
    valid File Extensions
    """
    spark: str = "spark"
    keras: str = "h5"
    h2o: str = "h2o"
    sklearn: str = "pkl"

    @staticmethod
    def get_valid() -> tuple:
        """
        Get valid file extensions
        :return: valid file extensions
        """
        return FileExtensions.spark, FileExtensions.keras, FileExtensions.h2o, FileExtensions.sklearn
