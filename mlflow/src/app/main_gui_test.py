from flask import Flask, Response, render_template as show_html

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2018, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Murray Brown", "Monte Zweben", "Ben Epstein"]

__license__: str = "Commerical"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"
__status__: str = "Quality Assurance (QA)"

app: Flask = Flask(__name__)


@app.route('/', methods=['GET'])
@app.route('/tracker', methods=['GET'])
def job_tracker_gui() -> Response:
    """
    Serves up Job Tracker GUI
    where users can view existing jobs and
    queue new ones.
    :return:
    """
    return show_html('index.html', chart_data=[1,2,3,4,5])


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
