import logging
import os

from pyspark.ml import PipelineModel, Pipeline
from splicemachine.ml import zeppelin
from splicemachine.spark import context

import mlflow

logging.basicConfig()
logger = logging.getLogger('retrainer')
logger.setLevel(logging.DEBUG)

mlflow.set_tracking_uri('/mlruns')
scheduled_run = zeppelin.Run()


class Trainer(object):
    def __init__(self, sc, sqlContext, task, queue):
        self.sc = sc
        self.sqlContext = sqlContext
        self.task = task
        self.queue = queue

    def train(self):
        new_pipeline = self.decompose_fitted_pipeline()
        self.create_mlflow_experiment()
        df = self.get_data_from_splicedb()
        metrics = self.build_classification_model(df, new_pipeline)
        should_deploy = self.check_deployment_threshold(metrics)
        return should_deploy

    def decompose_fitted_pipeline(self):
        fitted_pipeline_path = self.task.payload['ml_model_path'] + '/model'
        if not os.path.isdir(fitted_pipeline_path):
            raise Exception('Path: ' + fitted_pipeline_path + ' is not a valid pyspark '
                                                              'pipeline path')

        fitted_pyspark_pipeline = PipelineModel.load(fitted_pipeline_path)
        pipeline_stages = fitted_pyspark_pipeline.stages  # Extract pipeline steps from fitted
        # pipeline

        new_pipeline = Pipeline(stages=pipeline_stages)
        return new_pipeline

    def get_data_from_splicedb(self):
        splice = context.PySpliceContext(os.environ.get('JDBC_URL'), self.sqlContext)
        returned_df = splice.df(
            'SELECT * FROM {table}'.format(table=self.task.payload['source_table']))
        return returned_df

    def create_mlflow_experiment(self):
        experiment_name = (self.task.payload['app_name'] + '_scheduled').replace(' ', '_').lower()
        try:
            zeppelin.experiment_maker(experiment_name)
        except Exception as e:
            logger.error(e)
            raise Exception('Experiment {experiment} cannot be')

    def random_split_train_test(self, df):
        training_size = float(self.task.payload['train_size'])
        test_size = 1 - training_size
        train, test = df.randomSplit([training_size, test_size])
        scheduled_run.log_param('datasource', self.task.payload['source_table'])
        scheduled_run.log_param('train_num', str(train.count()))
        scheduled_run.log_param('test_num', str(test.count()))
        return train, test

    @staticmethod
    def evaluate_classification_model(predictions_df):
        evaluator = zeppelin.ModelEvaluator(confusion_matrix=True)
        evaluator.input(predictions_df)
        metrics_dict = evaluator.get_results(output_type='dict')

        for metric, calculated in metrics_dict:
            logger.debug(metric + ' --> ' + calculated)
            scheduled_run.log_metric(metric, calculated)

        return metrics_dict

    def build_classification_model(self, df, pipeline):
        train_df, test_df = self.random_split_train_test(df)
        fitted_model_pipeline = pipeline.fit(train_df)
        predicted_df = fitted_model_pipeline.transform(test_df)
        scheduled_run.log_param('estimator', str(pipeline.stages[-1]).split('_')[0])
        scheduled_run.log_model(fitted_model_pipeline, 'pysparkmodel')
        returned_metrics = self.evaluate_classification_model(predicted_df)
        return returned_metrics

    def check_deployment_threshold(self, metrics):
        if self.task.payload['deployment_mode'] == 'manual':
            return False
        elif self.task.payload['deployment_mode'] == 'automatic':
            return True
        elif self.task.payload['deployment_mode'] == 'threshold':
            avail_ops = ['==', '<', '>', '<=', '>=', '!=']
            avail_metrics = metrics.keys()
            m1, t, n = self.task.payload['deployment_thresh'].split(' ')
            if m1 in avail_metrics and t in avail_ops:
                try:
                    threshold = float(n)
                except ValueError:
                    return Exception('Malformed Threshold! Here is an example: specificity >= 0.6')

                output = eval(
                    '{metric} {op} {num}'.format(metric='metrics["' + m1 + '"]', op=t,
                                                 num=str(threshold)))
                return output
