"""
Model Types for database deployment
"""


class H2OModelType:  # see https://bit.ly/3gJ69gc
    """
    Model types for H2O Model deployment
    in the database
    """
    SINGLE_PRED_DOUBLE = 0  # (REGRESSION) Models that return a single Double value (Regression, HGLMRegression)
    SINGLE_PRED_INT = 1  # (SINGULAR) Models that return a single Int value (Clustering)
    MULTI_PRED_INT = 2  # (CLASSIFICATION) Models that only return N classes with probability values associated (Binomial, Multinomial, Ordinal)
    MULTI_PRED_DOUBLE = 3  # (KEY_VALUE) Models whose output labels are known (AutoEncoder, TargetEncoder, DimReduction, WordEmbedding,
    # AnomalyDetection)

class SparkModelType:
    """
    Spark Model types for MLeap Deployment to DB
    """
    SINGLE_PRED_DOUBLE = 0 # REGRESSION
    SINGLE_PRED_INT = 1 # CLUSTERING_WO_PROB
    MULTI_PRED_INT = 2 # CLUSTERING_WITH_PROB, CLASSIFICATION

    @staticmethod
    def get_class_supporting_types() -> set:
        """
        Get a set of types that support prediction classes
        :return: set of types
        """
        return {SparkModelType.MULTI_PRED_INT}


class SklearnModelType:
    """
    Model Types for Scikit-learn models
    Sklearn isn't as well defined in their model categories, so we are going to classify them by their return values
    """
    SINGLE_PRED_DOUBLE = 0 # REGRESSION
    SINGLE_PRED_INT = 1 # POINT_PREDICTION_CLF
    MULTI_PRED_DOUBLE = 2 # KEY_VALUE


class KerasModelType:
    """
    Model Types for Keras models
    """
    SINGLE_PRED_DOUBLE = 0 # REGRESSION
    MULTI_PRED_DOUBLE = 1 # KEY_VALUE


class DeploymentModelType:
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
    H2OModelType.MULTI_PRED_INT: DeploymentModelType.
}
