import json
import logging

import boto3
from flask import render_template, Flask, request, jsonify

logging.basicConfig()
LOGGER = logging.getLogger('dash')
LOGGER.setLevel(logging.DEBUG)

app = Flask(__name__)

client = boto3.client('sagemaker-runtime', region_name='us-east-2')


class Utils:

    @staticmethod
    def try_float(value):
        """
        Try to convert a value to a float
        :param value: value
        :return: floated version if it is possible, or regular value if not
        """
        try:
            return float(value)

        except ValueError:
            return value

    @staticmethod
    def invoke_sagemaker(request_payload, endpoint):
        """
        Invoke SageMaker Prediction Endpoint via boto3
        :param request_payload: json to send containing feature information
        :param endpoint: the endpoint name to send to
        :return:
        """
        sgm_request = client.invoke_endpoint(
            EndpointName=endpoint,
            Body=json.dumps(request_payload),
            ContentType='application/json',
            Accept='string'
        )

        garbage = ["[", "]", "'", "b"]

        response = Utils.eraser(garbage, str(sgm_request['Body'].read()))
        return float(response)

    @staticmethod
    def eraser(things_to_erase, string):
        """
        Delete everything in an array from a string
        :param things_to_erase: array containing strings to remove
        :param string: string to remove strings from
        :return: string without strings to remove
        """
        for thing in things_to_erase:
            string = string.replace(thing, '')

        return string


@app.route('/demo', methods=['GET', 'POST'])
@app.route('/', methods=['GET', 'POST'])
def demo():
    """
    Route for / or /demo. Sends a request to SageMaker on Deploy
    :return: html on get, process sagemaker and html on post
    """
    if request.method == 'GET':
        return render_template('index.html', predicted=False)
    else:
        payload = {}
        features = list(request.form.keys())
        if 'action' in features:
            features.remove('action')

        for feature in features:
            payload[feature] = [Utils.try_float(request.form[feature])]

        response = Utils.invoke_sagemaker(payload, 'bk')
        return render_template('index.html', predicted=True, prediction=response)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port='5000', debug=True)
