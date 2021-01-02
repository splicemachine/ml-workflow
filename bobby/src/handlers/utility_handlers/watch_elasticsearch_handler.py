"""
Contains code pertaining to watching scheduled jobs
that interact with ElasticSearch
"""
import re
from os import environ as env_vars
from time import sleep

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
<<<<<<< HEAD
from shared.logger.job_lifecycle_manager import JobLifecycleManager
from collections import defaultdict
=======
>>>>>>> cd24a56857fb87d159ab5490b1dc2e03c7baf4eb

from .base_utility_handler import BaseUtilityHandler

class WatchElasticSearchHandler(BaseUtilityHandler):
    """
    Watch a job from Elastic Search
    """
    DB_PASSWORD_REGEX = re.compile(env_vars['DB_PASSWORD'], flags=re.IGNORECASE)
    BUFFER_SIZE = 150

    def __init__(self, task_id: int) -> None:
        BaseUtilityHandler.__init__(self, task_id=task_id, logging_format=JobLifecycleManager.ELASTICSEARCH_LOGGING_FORMAT)
        self.elasticsearch = Elasticsearch(
            [env_vars.get('ELASTICSEARCH_URL', 'http://dev-elk-elasticsearch-client.splice-system.svc.local:9200')]
        )
        self.searcher = Search(using=self.elasticsearch)

    def locate_pod_name(self):
        """
        Locate the Pod Name from ElasticSearch
        """
        payload = self.task.parsed_payload
        pod_name = f'{payload["context_name"]}-{payload["entity_id"]}'

        if payload.get('job_name'):
            pod_name += f'-{payload["job_name"]}'

        results = self.searcher.query('match', **{'kubernetes.pod.name': pod_name}).execute()

        return results.hits[-1].kubernetes.pod.name

    def watch_logs(self):
        """
        Get the ElasticSearch Query from the payload
        """
        last_offset = 0
        pod_name = self.locate_pod_name()
        db_log_buffer = defaultdict()

        while True:
            results = self.searcher.query('match', **{'kubernetes.pod.name': pod_name}) \
                .filter('range', **{'log.offset': {'gt': last_offset}}).execute()

            last_offset = results.hits[-1].log.offset

            # Log the job messages
            for hit in reversed(results.hits):
                message = re.sub(WatchElasticSearchHandler.DB_PASSWORD_REGEX, "*****", hit.message)
                self.logger.info(message, send_db=True)
                if self.completion_regex.search(message):
                    break
                if self.failure_regex.search(message):
                    raise Exception("Job did not succeed! Failing...")


    def execute(self) -> None:
        """
        Watch the Job until completion
        :return:
        """
        self.completion_regex = re.compile("|".join(self.task.parsed_payload['completion_msgs']))
        self.failure_regex = re.compile("|".join(self.task.parsed_payload['failure_msgs']))
        self.watch_logs()
