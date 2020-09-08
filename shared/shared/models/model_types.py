"""
Model Types for database deployment
"""
from enum import Enum


class Representations:
    """
    Model Representations
    """
    # ~ GENERAL REPRESENTATIONS ~
    LIBRARY = 'library'
    BYTES = 'serialized'

    # ~ LIBRARY SPECIFIC REPRESENTATIONS ~ #
    # H2O
    RAW_MOJO = 'raw_mojo'
    JAVA_MOJO = 'java_mojo'

    # Spark
    MLEAP = 'mleap'


class Metadata:
    """
    Model Metadata Values
    """
    CLASSES = 'classes'
    FILE_EXT = 'file_ext'
    TYPE = 'type'
    GENERIC_TYPE = 'generic_type'

    DATAFRAME_EXAMPLE = 'dataframe_example'
    SQL_SCHEMA = 'sql_schema'
    SCHEMA_STR = 'schema_str'
    MODEL_VECTOR_STR = 'model_vector_str' # Like SCHEMA_STR but only columns that are in the feature vector


class H2OModelType(Enum):  # see https://bit.ly/3gJ69gc
    """
    Model types for H2O Model deployment
    in the database
    """
    SINGLE_PRED_DOUBLE = 0  # (REGRESSION) Models that return a single Double value (Regression, HGLMRegression)
    SINGLE_PRED_INT = 1  # (SINGULAR) Models that return a single Int value (Clustering)
    MULTI_PRED_INT = 2  # (CLASSIFICATION) Models that only return N classes with probability values associated
    # (Binomial, Multinomial, Ordinal)
    MULTI_PRED_DOUBLE = 3  # (KEY_VALUE) Models whose output labels are known
    # (AutoEncoder, TargetEncoder, DimReduction, WordEmbedding, AnomalyDetection)


class SparkModelType(Enum):
    """
    Spark Model types for MLeap Deployment to DB
    """
    SINGLE_PRED_DOUBLE = 0  # REGRESSION
    SINGLE_PRED_INT = 1  # CLUSTERING_WO_PROB
    MULTI_PRED_INT = 2  # CLUSTERING_WITH_PROB, CLASSIFICATION


class SklearnModelType(Enum):
    """
    Model Types for Scikit-learn models
    Sklearn isn't as well defined in their model categories, so we are going to classify them by their return values
    """
    SINGLE_PRED_DOUBLE = 0  # REGRESSION
    SINGLE_PRED_INT = 1  # POINT_PREDICTION_CLF
    MULTI_PRED_DOUBLE = 2  # KEY_VALUE


class KerasModelType(Enum):
    """
    Model Types for Keras models
    """
    SINGLE_PRED_DOUBLE = 0  # REGRESSION
    MULTI_PRED_DOUBLE = 1  # KEY_VALUE


class DeploymentModelType(Enum):
    """
    Generic Model Types for Deployments
    """
    SINGLE_PRED_DOUBLE = 0
    SINGLE_PRED_INT = 1
    MULTI_PRED_INT = 2
    MULTI_PRED_DOUBLE = 3


class ModelTypeMapper:
    """
    Class for mapping class model type to deployment model type
    """

    @staticmethod
    def get_model_type(model_type) -> DeploymentModelType:
        return model_mapping[model_type]


model_mapping = {
    H2OModelType.SINGLE_PRED_DOUBLE: DeploymentModelType.SINGLE_PRED_DOUBLE,
    H2OModelType.SINGLE_PRED_INT: DeploymentModelType.SINGLE_PRED_INT,
    H2OModelType.MULTI_PRED_INT: DeploymentModelType.MULTI_PRED_INT,
    H2OModelType.MULTI_PRED_DOUBLE: DeploymentModelType.MULTI_PRED_DOUBLE,
    SklearnModelType.SINGLE_PRED_DOUBLE: DeploymentModelType.SINGLE_PRED_DOUBLE,
    SklearnModelType.SINGLE_PRED_INT: DeploymentModelType.SINGLE_PRED_INT,
    SklearnModelType.MULTI_PRED_DOUBLE: DeploymentModelType.MULTI_PRED_DOUBLE,
    SparkModelType.SINGLE_PRED_DOUBLE: DeploymentModelType.SINGLE_PRED_DOUBLE,
    SparkModelType.SINGLE_PRED_INT: DeploymentModelType.SINGLE_PRED_INT,
    SparkModelType.MULTI_PRED_INT: DeploymentModelType.MULTI_PRED_INT,
    KerasModelType.SINGLE_PRED_DOUBLE: DeploymentModelType.SINGLE_PRED_DOUBLE,
    KerasModelType.MULTI_PRED_DOUBLE: DeploymentModelType.MULTI_PRED_DOUBLE
}
