from typing import Optional, List

from tensorflow.keras import Model as KerasModel

from shared.models.model_types import KerasModelType


class KerasUtils:
    """
    Utilities for preparing the metadata of keras models
    """

    @staticmethod
    def get_keras_model_type(model: KerasModel, pred_threshold: float) -> KerasModelType:
        """
        Keras models are either Key value returns or "regression" (single value)
        If the output layer has multiple nodes, or there is a threshold (for binary classification), it will
        be a key value return because n values will be returned (n = # output nodes + 1)
        :param model: (KerasModel)
        :param pred_threshold: (float) the prediction threshold if one is passed
        :return: (KerasModelType)
        """
        return KerasModelType.KEY_VALUE if model.layers[-1].output_shape[
                                               -1] > 1 or pred_threshold else KerasModelType.REGRESSION

    @staticmethod
    def validate_keras_model(model: KerasModel, specified_classes: Optional[List[str]] = None):
        """
        Right now, we only support feed forward models. Vector inputs and Vector outputs only
        When we move to LSTMs we will need to change that
        :param model: model to validate
        :param specified_classes: if classes are specified
        :return: nothing unless model is invalid
        """
        input_shape = model.layers[0].input_shape
        output_shape = model.layers[-1].output_shape

        if (len(input_shape) != 2 or len(output_shape) != 2) or (input_shape[0] or output_shape[0]):
            raise Exception("We currently only support feed-forward models. The input and output shapes"
                            "of the models must be (None, #). Please raise an issue here: https://github.com"
                            "/splicemachine/pysplice/issues")

        if specified_classes and len(specified_classes) != output_shape[-1]:
            raise Exception("The number of classes does not match the output shape of your model")
