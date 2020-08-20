from inspect import signature as get_signature

from sklearn.base import BaseEstimator as ScikitModel
from sklearn.base import ClassifierMixin as ClassifierModel
from sklearn.base import ClusterMixin as ClusteringModel
from sklearn.base import RegressorMixin as RegressionModel
from sklearn.pipeline import Pipeline as SKPipeline

from shared.models.model_types import SklearnModelType


class ScikitUtils:
    """
    Utilities for Sckit-learn model metadata
    preparation
    """

    @staticmethod
    def locate_model_stage(pipeline):
        """
        Locate the model stage within an Sklearn pipepline
        :param pipeline: Skpipeline
        :return: model stage if exsts, else None
        """
        raise NotImplementedError("not implemented :(")

    @staticmethod
    def validate_scikit_args(model, lib_specific_args):
        """
        Validate scikit-learn arguments passed in

        :param model: the Sklearn model passed in
        :param lib_specific_args: dictionary of library specific arguments
            passed in
        :return:
        """
        keys = set(lib_specific_args.keys())
        if keys - {'predict_call', 'predict_args'} != set():
            raise Exception("You've passed in an library specific key that is not valid. Valid keys are "
                            "('predict_call', 'predict_args')")
        elif lib_specific_args.get('predict_call'):
            predict_call = lib_specific_args['predict_call']
            if not hasattr(model, predict_call):
                raise Exception('The predict function specified is not available for the given model')
            if predict_call != 'predict' and lib_specific_args['predict_args']:
                raise Exception(f'predict_args passed in but predict_call is {predict_call}. '
                                f'This combination is not allowed')
        if lib_specific_args.get('predict_args'):
            predict_args = lib_specific_args['predict_args']
            if predict_args not in ('return_std', 'return_cov') and not isinstance(model, SKPipeline):
                raise Exception('Predict_args value is invalid. Available options are ("return_std", "return_cov")')
            else:
                # TOdo implement the location
                model_stage = model.steps[-1][-1] if isinstance(model, SKPipeline) else model
                model_params = get_signature(model_stage.predict) if \
                    hasattr(model_stage, 'predict') else get_signature(model_stage.transform)

                if predict_args not in model_params.parameters:
                    raise Exception("Predict arg specified is not available for this model!")

        return None if lib_specific_args.get('predict_call') == 'predict' and 'predict_args' not in \
                       lib_specific_args else lib_specific_args

    @staticmethod
    def get_model_type(model, lib_specific_args):
        """
        Get the model type of an sklearn model/pipeline
        :param model: model to get the type of
        :param lib_specific_args: library specific arguments
        :return: model type
        """
        if lib_specific_args:
            return SklearnModelType.MULTI_PRED_DOUBLE

        steps = model.steps if isinstance(model, SKPipeline) else [None, model]

        for _, step in reversed(steps):
            if not isinstance(step, ScikitModel):
                continue
            if hasattr(model, 'predict'):
                if isinstance(step, (ClassifierModel, ClusteringModel)):
                    return SklearnModelType.SINGLE_PRED_INT
                elif isinstance(step, RegressionModel):
                    return SklearnModelType.SINGLE_PRED_DOUBLE
                raise Exception(f"Unknown model type {type(model)}")
            elif hasattr(model, 'transform'):
                return SklearnModelType.MULTI_PRED_DOUBLE
        else:
            raise Exception("Couldn't locate a model that can be predicted/transformed")
