"""
Model Types for database deployment
"""


class H2OModelType:  # see https://bit.ly/3gJ69gc
    """
    Model types for H2O Model deployment
    in the database
    """
    REGRESSION = 0  # Models that return a single Double value (Regression, HGLMRegression)
    SINGULAR = 1  # Models that return a single Int value (Clustering)
    CLASSIFICATION = 2  # Models that only return N classes with values associated (Binomial, Multinomial, Ordinal)
    KEY_VALUE = 3  # Models whose output labels are known (AutoEncoder, TargetEncoder, DimReduction, WordEmbedding,
    # AnomalyDetection)


class SparkModelType:
    """
    Spark Model types for MLeap Deployment to DB
    """
    CLASSIFICATION = 0
    REGRESSION = 1
    CLUSTERING_WITH_PROB = 2
    CLUSTERING_WO_PROB = 3

    @staticmethod
    def get_class_supporting_types() -> set:
        """
        Get a set of types that support prediction classes
        :return: set of types
        """
        return {SparkModelType.CLASSIFICATION, SparkModelType.CLUSTERING_WITH_PROB}


class SklearnModelType:
    """
    Model Types for Scikit-learn models
    Sklearn isn't as well defined in their model categories, so we are going to classify them by their return values
    """
    REGRESSION = 0
    POINT_PREDICTION_CLF = 1
    KEY_VALUE = 2


class KerasModelType:
    """
    Model Types for Keras models
    """
    REGRESSION = 0
    KEY_VALUE = 1
