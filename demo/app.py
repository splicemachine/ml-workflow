import logging

from flask import render_template, Flask, request, jsonify

logging.basicConfig()
logger = logging.getLogger('dash')
logger.setLevel(logging.DEBUG)

app = Flask(__name__)


@app.route('/demo', methods=['GET', 'POST'])
@app.route('/', methods=['GET', 'POST'])
def demo():
    if request.method == 'GET':
        return render_template('index.html')
    else:
        return jsonify(request.form)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port='5000', debug=True)
