from flask import Flask, request, jsonify

from splicemachine_queue import SpliceMachineQueue

app = Flask(__name__)

queue = SpliceMachineQueue()


# TODO remove enqueue create table
@app.route('/deploy', methods=['POST'])
def deploy():
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
        'app_name': json_returned['app_name']
    }

    job_id = queue.enqueue('deploy', assembled_metadata)
    return jsonify({'job_id': job_id, 'status': 'pending'})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
