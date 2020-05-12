"""
Our version of the MLFlow tracking server, which has authorization
"""
from functools import partial as fix_params

from mlflow.server import app as APP, STATIC_DIR, send_from_directory
from mlflow.server.handlers import _add_static_prefix
from flask_login import LoginManager, login_user, logout_user, login_required, current_user

from mlmanager_lib import CloudEnvironments
from .utilities.utils import *

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Murray Brown", "Monte Zweben", "Ben Epstein"]

__license__: str = "Commercial"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"
__status__: str = "Quality Assurance (QA)"

CLOUD_ENVIRONMENT: CloudEnvironment = CloudEnvironments.get_current()


@APP.route(_add_static_prefix('/'))
@login_required
def index():
    """
    Load MLFlow UI
    :return: MLFLow UI HTML
    """
    return send_from_directory(STATIC_DIR, 'index.html')


CREATE_GLOBAL_JINJA_VARIABLES = APP.context_processor()(fix_params(create_global_jinja_variables, CLOUD_ENVIRONMENT))
LOGIN = APP.route('/login', methods=['GET', 'POST'])(login)
LOGOUT = APP.route('/logout')(logout)
USER_LOADER = LoginManager.user_loader(user_loader)
UNAUTHORIZED_USER = LoginManager.unauthorized_handler(unauthorized_user)

if __name__ == "__main__":
    APP.run(host='0.0.0.0', port=5000)
