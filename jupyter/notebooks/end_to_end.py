"""
How Do I Run this Script?
-------------------------
1) First Run the MLManager Stack via Docker-Compose
2) SSH into the Jupyter Container via `docker exec -it [...] bash`
3) Run this command `/opt/conda/envs/beakerx/bin/python /opt/notebooks/end_to_end.py`
4) You should be Good to Go :)
"""

import logging
from functools import partial
from os import environ
from typing import Any

import plotly.express as px
from py4j.java_gateway import java_import
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.sql import SparkSession, DataFrame
from splicemachine.ml.management import MLManager
from splicemachine.ml.utilities import SpliceMultiClassificationEvaluator
from sqlalchemy import create_engine, String, Float, text, Binary, bindparam

environ['USER'] = 'amrit'
environ['PYSPARK_PYTHON'] = 'python3.6'
environ['PYSPARK_DRIVER_PYTHON'] = 'python3.6'

logger = logging.getLogger(__name__)


class Spark:
    """:
    Spark Object Namespace
    """
    logger.debug("Creating Spark Session...")
    session = SparkSession.builder.getOrCreate()
    logger.debug("Done.")
    sc = session._sc
    sqlContext = session._wrapped


class Mocked:
    """
    Mocked Objects Namespace
    """

    class PySpliceContext:
        """
        Mock PySpliceContext for Local Testing
        """

        def __init__(self, spark_session: Any):
            """
            :param spark_session: Any object to match constructor
            """
            self.sc: Any = Spark.sc
            self.jvm: Any = Spark.sc._jvm

        def df(self, sql: str) -> Any:
            """
            Returns the Iris formatted dataset
            from plotly
            :param sql: (str) a mocked SQL query for demo purposes

            """
            return Spark.sqlContext.createDataFrame(px.data.iris())

    @staticmethod
    def log_spark_model(self: MLManager, model: PipelineModel, model_dir='model'):
        logger.info("Storing Spark Model via SQLAlchemy in Database")

        java_import(Spark.sc._jvm, 'java.io.{ByteArrayOutputStream, ObjectOutputStream}')
        java_import(Spark.sc._jvm, 'java.io.{ByteArrayInputStream, ObjectInputStream}')
        jvm = Spark.sc._jvm
        baos = jvm.java.io.ByteArrayOutputStream()  # serialize the PipelineModel to a byte array
        oos = jvm.java.io.ObjectOutputStream(baos)
        oos.writeObject(model._to_java())
        oos.flush()
        oos.close()

        bytes_array = baos.toByteArray()

        engine = create_engine('splicemachinesa://splice:admin@host.docker.internal:1527/splicedb')

        sql = text("INSERT INTO ARTIFACTS VALUES (:run_id, :name, :size, :binary)")

        sql = sql.bindparams(
            bindparam("run_id", type_=String),
            bindparam("name", type_=String),
            bindparam("size", type_=Float),
            bindparam("binary", type_=Binary)
        )
        self.set_tag('splice.model_name', model_dir)

        engine.execute(sql, {
            "run_id": self.current_run_id,
            "name": model_dir,
            "size": 0,
            "binary": bytes_array
        })
        logger.info("Done.")


class EndToEndTester:
    """
    Running the MLManager Stack End to End
    """

    def __init__(self):
        self.splice = Mocked.PySpliceContext(Spark.session)
        self.manager = MLManager(self.splice, _testing=True)
        self.manager.log_spark_model = partial(Mocked.log_spark_model, self.manager)

        self.manager.start_run(tags={
            'team': 'cloud',
            'project': 'iris'
        })  # start a run with some associated metadata (these will be translated into tags)

        self._ba_creds = ('splice', 'admin')  # Basic-Auth Creds

    def get_dataframe(self) -> DataFrame:
        """
        :return: Iris DataFrame for Spark Use
        """
        logger.info("Getting DataFrame")
        df: DataFrame = self.splice.df("SELECT * FROM SPLICE.IRIS_DEMO").drop('species_id')
        self.manager.log_param('dataset', 'iris')  # we can also use shortcut
        self.manager.lp('total_size', df.count())
        self.manager.log_params(
            [
                ('dataset_source', 'scikit-learn'),
                ('table', 'SPLICE.IRIS_DEMO')
            ]
        )
        logger.info("Done.")
        return df

    def build_preprocessing_pipeline(self) -> Pipeline:
        """
        :return: Preprocessing Pipeline
        """
        logger.info("Building Pipeline")
        dataframe: DataFrame = self.get_dataframe()
        features: list = dataframe.columns
        features.remove('species')  # this is our label
        pipeline: Pipeline = Pipeline(stages=[
            StringIndexer(inputCol='species',
                          outputCol='label'),  # convert strings to categorical
            VectorAssembler(inputCols=features,
                            outputCol='features'),
            RandomForestClassifier()
        ])

        self.manager.log_feature_transformations(pipeline)  # log column-level feature transforms
        logger.info("Done.")
        return dataframe, pipeline

    def build_and_predict_model(self) -> DataFrame:
        """
        :return: Prediction DataFrame
        """
        logger.info("Building and Predicting Model")
        dataframe, pipeline = self.build_preprocessing_pipeline()
        train, test = dataframe.randomSplit([0.75, 0.25])
        self.manager.start_timer("Training")
        fitted_pipeline: PipelineModel = pipeline.fit(train)
        self.manager.log_and_stop_timer()

        self.manager.log_model_params(fitted_pipeline)  # automatically log model hyperparameters
        self.manager.log_spark_model(
            fitted_pipeline)  # save pipeline to the database for deployment

        self.manager.start_timer("Prediction")
        predictions: DataFrame = fitted_pipeline.transform(test)
        self.manager.log_and_stop_timer()
        logger.info("Done.")
        return predictions

    def evaluate_model(self) -> None:
        logger.info("Evaluating Model")
        predictions: DataFrame = self.build_and_predict_model()
        evaluator: SpliceMultiClassificationEvaluator = SpliceMultiClassificationEvaluator(
            Spark.session)
        evaluator.input(predictions)
        self.manager.log_evaluator_metrics(evaluator)
        logger.info("Done.")

    def deploy_aws(self) -> None:
        logger.info("Deploying Model to AWS")
        self.manager.login_director(*self._ba_creds)
        self.manager.deploy_aws('irisappdemo')
        self.manager.end_run()
        logger.info("Done.")


def main():
    """
    Main Application Logic.
    Stops Spark Session after E-E finishes
    on error or on success
    """
    try:
        tester: EndToEndTester = EndToEndTester()
        tester.build_and_predict_model()
        tester.deploy_aws()
    except:
        logger.exception("Encountered Error while Running End to End")
    finally:
        Spark.session.stop()


if __name__ == "__main__":
    main()
