import logging
import random
import sys
import os
from hashlib import md5

from flask import Flask, request, jsonify, make_response

from splicemachine_queue import SpliceMachineQueue


__author__ = "Splice Machine, Inc."
__copyright__ = "Copyright 2018, Splice Machine Inc. All Rights Reserved"
__credits__ = ["Amrit Baveja", "Murray Brown", "Monte Zweben", "Ben Epstein"]

__license__ = "Commerical"
__version__ = "2.0"
__maintainer__ = "Amrit Baveja"
__email__ = "abaveja@splicemachine.com"
__status__ = "Quality Assurance (QA)"


app = Flask(__name__)

queue = SpliceMachineQueue()

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s (%(lineno)s) - %(levelname)s: %(message)s",
                    datefmt='%Y.%m.%d %H:%M:%S')
logger = logging.getLogger('job_handler_api')
logger.setLevel(logging.DEBUG)

available_actions = ['start', 'stop']
available_handlers = ['deploy', 'schedule', 'retrain']


def failed_response(*args, **kwargs):
    return make_response(jsonify(*args, **kwargs), 404)


def parse_exception():
    """
    Format an exception to look nice in a string format
    :return: exception string formatted
    """
    found_exception = sys.exc_info()
    exception_class, exception_msg = str(found_exception[0]), str(found_exception[1])

    class_function_string = exception_class.split('class')[1][:-1]
    message = '"' + exception_msg + '"'
    return '{exception_func}({message})'.format(exception_func=class_function_string,
                                                message=message)


def __generate_retrain_template(json_returned, handler):
    """
    :param json_returned: json from request
    :param handler: the job handler
    :return: dict containing assembled metadata
    """
    if handler == 'schedule_start':
        return {
            'handler': handler,
            'ml_model_path': json_returned['path'] + json_returned['postfix'],
            'deployment_thresh': json_returned.get('deployment_thresh', 'none'),
            'when_to_deploy': json_returned['when_to_deploy'],
            'interval': json_returned['interval'],
            'problem_type': json_returned['problem_type'],
            'deployment_mode': json_returned['deployment_mode'],
            'instance_count': json_returned['instance_count'],
            'sagemaker_region': json_returned.get('sagemaker_region', 'none'),
            'app_name': json_returned.get('app_name', 'none'),
            'instance_type': json_returned.get('instance_type', 'none'),
            'iam_role': json_returned.get('iam_role', 'none'),
            'source_table': json_returned.get('source_table', 'none'),
            'train_size': json_returned['train_size'],
            'random_string': str(md5(str(random.randint(0, 10 ** 5)).encode('utf-8')).hexdigest())

        }
    elif handler == 'retrain':
        return {
            'handler': handler,
            'ml_model_path': json_returned['path'] + json_returned['postfix'],
            'deployment_thresh': json_returned.get('deployment_thresh', 'none'),
            'when_to_deploy': json_returned['when_to_deploy'],
            'problem_type': json_returned['problem_type'],
            'deployment_mode': json_returned['deployment_mode'],
            'instance_count': json_returned['instance_count'],
            'sagemaker_region': json_returned.get('sagemaker_region', 'none'),
            'app_name': json_returned.get('app_name', 'none'),
            'instance_type': json_returned.get('instance_type', 'none'),
            'iam_role': json_returned.get('iam_role', 'none'),
            'source_table': json_returned.get('source_table', 'none'),
            'train_size': json_returned['train_size'],
            'random_string': str(md5(str(random.randint(0, 10 ** 5)).encode('utf-8')).hexdigest())

        }
    else:
        return None


@app.route('/service/<service>/<action>', methods=['POST'])
def service_handler(service, action):
    """
    Stop/Start another service
    :param service: service handler to stop/start
    :param action: stop/start
    :return: success/error json
    """
    if action in ['start', 'stop']:
        assembled_metadata = {
            'service': service,
            'action': action,
            'handler': action + '_service',
            'random_string': str(md5(str(random.randint(0, 10 ** 5)).encode('utf-8')).hexdigest())
        }
        job_id = queue.enqueue(action + '_service', assembled_metadata)

        return jsonify(job_id=job_id, status='pending update')

    return failed_response(status='failed',
                           msg='action must be in [start, stop]. please format url correctly')


@app.route('/schedule/<action>/<handler>', methods=['POST'])
def scheduler_handler(action, handler):
    """
    Schedule a job to repeat via metrenome
    :param action: start/stop
    :param handler: request handler to trigger
    :return: success/error json
    """
    try:
        json_returned = request.get_json()

        if action not in available_actions:
            return failed_response(status='failed', msg='Action {action} is not available'.format(
                action=action))
        elif handler not in available_handlers:
            return failed_response(status='failed', msg='Handler {handler} is not available'.format(
                handler=handler))
        else:
            if handler == 'retrain':
                if action == 'start':
                    assembled_metadata = __generate_retrain_template(json_returned,
                                                                     'schedule_start')
                    job_id = queue.enqueue('schedule_start', assembled_metadata)
                    return jsonify({'job_id': job_id, 'status': 'pending'})

                elif action == 'stop':
                    assembled_metadata = {
                        'handler': 'schedule_stop',
                        'job_id': json_returned['job_id']
                    }
                    job_id = queue.enqueue('schedule_stop', assembled_metadata)
                    return jsonify({'job_id': job_id, 'status': 'pending'})

    except Exception as e:
        logger.error(e)
        return failed_response(status='failed', msg=parse_exception())


@app.route('/retrain', methods=['POST'])
def retrain_handler():
    """
    Retrain a model
    :return: success/error json
    """
    try:
        json_retrained = request.get_json()

        assembled_metadata = __generate_retrain_template(json_retrained, 'retrain')

        if assembled_metadata:
            job_id = queue.enqueue('retrain', assembled_metadata)
            return jsonify({'job_id': job_id, 'status': 'pending'})

        return failed_response(status='failed', msg='invalid handler!')

    except Exception as e:
        logger.error(e)
        return failed_response(status='failed', msg=parse_exception())


@app.route('/deploy', methods=['POST'])
def deploy_handler():
    """
    Deployment: Create a deployment job in Splice Machine Queue
    :return:
    """
    try:
        json_returned = request.get_json()
        logger.info(json_returned)

        assembled_metadata = {
            'handler': 'deploy',
            'experiment_id': json_returned['experiment_id'],
            'run_id': json_returned['run_id'],
            'postfix': json_returned['postfix'],
            'sagemaker_region': json_returned['region'],
            'instance_type': json_returned['instance_type'],
            'instance_count': json_returned['instance_count'],
            'iam_role': os.environ['SAGEMAKER_ROLE'],
            'deployment_mode': json_returned['deployment_mode'],
            'app_name': json_returned['app_name'],
            'random_string': str(md5(str(random.randint(0, 10 ** 5)).encode('utf-8')).hexdigest())

        }
        job_id = queue.enqueue('deploy', assembled_metadata)
        return jsonify(job_id=job_id, status='pending')

    except Exception as e:
        logger.info(e)
        return failed_response(status='failed', msg=parse_exception())


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
