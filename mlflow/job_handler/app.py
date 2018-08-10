from flask import Flask, request, jsonify

from splicemachine_queue import SpliceMachineQueue

app = Flask(__name__)

queue = SpliceMachineQueue()

available_actions = ['start', 'stop']
available_handlers = ['deploy', 'schedule', 'retrain']


def __generate_retrain_template(json_returned, handler):
    return {
        'handler': handler,
        'ml_model_path': json_returned['path'] + json_returned['postfix'],
        'deployment_thresh': json_returned['deployment_thresh'],
        'interval': json_returned['interval'],
        'user_defined_metrics': json_returned['user_defined_metrics'],
        'deployment_mode': json_returned['deployment_mode'],
        'instance_count': json_returned['instance_count'],
        'sagemaker_region': json_returned['sagemaker_region'],
        'app_name': json_returned['app_name'],
        'instance_type': json_returned['instance_type'],
        'iam_role': json_returned['iam_role'],
        'source_table': json_returned['source_table']
    }


@app.route('/schedule/<action>/<handler>', methods=['POST'])
def scheduler_handler(action, handler):
    json_returned = request.get_json()

    if action not in available_actions:
        return jsonify(status='failed', msg='Action {action} is not available'.format(
            action=action))
    elif handler not in available_handlers:
        return jsonify(status='failed', msg='Handler {handler} is not available'.format(
            handler=handler))
    else:
        if handler == 'train':
            if action == 'start':
                assembled_metadata = __generate_retrain_template(json_returned, 'schedule_start')
                job_id = queue.enqueue('schedule_start', assembled_metadata)
                return jsonify({'job_id': job_id, 'status': 'pending'})

            elif action == 'stop':
                assembled_metadata = {
                    'handler': 'schedule_stop',
                    'job_id': json_returned['job_id']
                }
                job_id = queue.enqueue('schedule_stop', assembled_metadata)
                return jsonify({'job_id': job_id, 'status': 'pending'})


@app.route('/retrain', methods=['POST'])
def retrain_handler():
    json_retrained = request.get_json()

    assembled_metadata = __generate_retrain_template(json_retrained, 'retrain')

    job_id = queue.enqueue('retrain', assembled_metadata)
    return jsonify({'job_id': job_id, 'status': 'pending'})


@app.route('/deploy', methods=['POST'])
def deploy_handler():
    """
    Deployment: Create a deployment job in Splice Machine Queue
    :return:
    """
    json_returned = request.get_json()
    postfix = json_returned['postfix']

    assembled_metadata = {
        'handler': 'deploy',
        'ml_model_path': json_returned['path'] + postfix,
        'sagemaker_region': json_returned['region'],
        'instance_type': json_returned['instance_type'],
        'instance_count': json_returned['instance_count'],
        'iam_role': json_returned['iam_role'],
        'deployment_mode': json_returned['deployment_mode'],
        'app_name': json_returned['app_name']
    }

    job_id = queue.enqueue('deploy', assembled_metadata)
    return jsonify({'job_id': job_id, 'status': 'pending'})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
