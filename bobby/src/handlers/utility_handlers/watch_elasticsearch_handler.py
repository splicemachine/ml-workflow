"""
Contains code pertaining to watching scheduled jobs
that interact with ElasticSearch
"""
from os import environ as env_vars

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

from .base_utility_handler import BaseUtilityHandler


class WatchElasticSearchHandler(BaseUtilityHandler):
    """
    Watch a job from Elastic Search
    """

    def __init__(self, task_id: int) -> None:
        BaseUtilityHandler.__init__(self, task_id=task_id)
        self.elasticsearch = Elasticsearch(
            [env_vars.get('ELASTICSEARCH_URL', 'http://dev-elk-elasticsearch-client:9200')]
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

        results = self.searcher.query('match', **{'kubernetes.pod.name': pod_name})\
            .sort("timestamp", {'order', "desc"}).execute()

        return results.hits[-1]

    def watch_logs(self):
        """
        Get the ElasticSearch Query from the payload
        """
        last_timestamp = 0
        pod_name = self.locate_pod_name()

        while True:
            results = self.searcher.query('match', {'kubernetes.pod.name': pod_name}) \
                .filter('range', **{'@timestamp': {'gte': last_timestamp}}).execute()
            last_timestamp = getattr(results.hits[-1], '@timestamp')
            messages = [results.hits[i].message for i in range(len(results.hits))]

            for completion_message in self.task.parsed_payload['completion_msgs']:
                for message in messages:
                    if completion_message in message:
                        break

            for failure_message in self.task.parsed_payload['failure_msgs']:
                for message in messages:
                    if failure_message in message:
                        raise Exception("Job did not succeed! Failing...")

    def execute(self) -> None:
        """
        Watch the Job until completion
        :return:
        """
        self.watch_logs()
