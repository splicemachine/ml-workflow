from shared.models.model_types import H2OModelType


class H2OUtils:
    """
    Utilities for preparing the metadata of H2O Models
    """

    @staticmethod
    def get_model_category(h2omojo) -> str:
        """
        Get the category of a specified H2O Mojo model
        :param h2omojo: the h2o mojo to get category of
        :return: category as string
        """
        return h2omojo.getModelCategory().toString()

    @staticmethod
    def get_category_type(category: str):
        """
        Get the model category type from the specified category
        :param category: category extracted by `get_model_category`
        :return: model type
        """
        if category == 'Regression':
            return H2OModelType.SINGLE_PRED_DOUBLE
        elif category in {'HGLMRegression', 'Clustering'}:
            return H2OModelType.SINGLE_PRED_INT
        elif category in {'Binomial', 'Multinomial', 'Ordinal'}:
            return H2OModelType.MULTI_PRED_INT
        elif category in {'AutoEncoder', 'TargetEncoder', 'DimReduction', 'WordEmbedding', 'AnomalyDetection'}:
            return H2OModelType.MULTI_PRED_DOUBLE
        raise Exception(f"H2O model {category} is not supported! Only models with MOJOs are currently supported.")
