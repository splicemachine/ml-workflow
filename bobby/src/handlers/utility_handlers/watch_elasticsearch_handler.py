"""
Contains code pertaining to watching scheduled jobs
that interact with ElasticSearch
"""
import re
from os import environ as env_vars

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from time import sleep
from retrying import retry

from .base_utility_handler import BaseUtilityHandler

class WatchElasticSearchHandler(BaseUtilityHandler):
    """
    Watch a job from Elastic Search
    """
    DB_PASSWORD_REGEX = re.compile(env_vars['DB_PASSWORD'], flags=re.IGNORECASE)

    def __init__(self, task_id: int) -> None:
        BaseUtilityHandler.__init__(self, task_id=task_id,
                                    logging_buffer_size=30,
                                    logging_format="{message}")  # log message already includes level + timestamp

        self.elasticsearch = Elasticsearch(
            [env_vars.get('ELASTICSEARCH_URL', 'http://dev-elk-elasticsearch-client.splice-system.svc.local:9200')]
        )
        self.searcher = Search(using=self.elasticsearch)

        self.completion_regex: re.Pattern = None
        self.failure_regex: re.Pattern = None

    def _retry_on_index_error(exception):
        """
        Return true if the exception is an IndexError
        """
        return isinstance(exception, IndexError)

    # We add retrying here because the pod may be pulling and might not exist yet, so the query won't return anything
    @retry(retry_on_exception=_retry_on_index_error, wait_exponential_multiplier=1000,
           wait_exponential_max=10000, stop_max_delay=600000)  # Max 10 min
    def locate_pod_name(self):
        """
        Locate the Pod Name from ElasticSearch
        """
        payload = self.task.parsed_payload
        pod_name = f'{payload["context_name"]}-{payload["entity_id"]}'

        if payload.get('job_name'):
            pod_name += f'-{payload["job_name"]}'

        results = self.searcher.query('match_phrase', **{'kubernetes.pod.name': pod_name}) \
                .sort("-@timestamp").execute()

        return results.hits[-1].kubernetes.pod.name

    def watch_logs(self):
        """
        Get the ElasticSearch Query from the payload
        """
        last_offset = 0
        pod_name = self.locate_pod_name()

        while True:
            sleep(0.1)
            results = self.searcher.query('match_phrase', **{'kubernetes.pod.name': pod_name}) \
                .filter('range', **{'log.offset': {'gt': last_offset}}).execute()

            if not results.hits:
                continue

            last_offset = results.hits[-1].log.offset

            # Log the job messages
            for hit in results.hits:
                message = re.sub(WatchElasticSearchHandler.DB_PASSWORD_REGEX, "*****", hit.message)
                self.logger.info(message, send_db=True)
                if self.completion_regex.search(message):
                    return
                if self.failure_regex.search(message):
                    raise Exception("Job did not succeed! Failing...")

    def execute(self) -> None:
        """
        Watch the Job until completion
        :return:
        """
        self.completion_regex = re.compile("|".join(self.task.parsed_payload['completion_msgs']), flags=re.I)
        self.failure_regex = re.compile("|".join(self.task.parsed_payload['failure_msgs']), flags=re.I)
        self.watch_logs()
