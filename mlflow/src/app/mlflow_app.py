"""
Our version of the MLFlow tracking server, which has authorization
"""
from functools import partial as fix_params

from mlflow.server import app as APP, STATIC_DIR, send_from_directory
from flask_login import LoginManager, login_user, logout_user, login_required, current_user

from mlmanager_lib import CloudEnvironments
from utilities.utils import *

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Murray Brown", "Monte Zweben", "Ben Epstein"]

__license__: str = "Commercial"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"
__status__: str = "Quality Assurance (QA)"

APP.config['SECRET_KEY']: str = "B1gd@t@4U!"  # cookie encryption
CLOUD_ENVIRONMENT: CloudEnvironment = CloudEnvironments.get_current()
LOGIN_MANAGER: LoginManager = LoginManager(APP)  # session-based user authentication


@APP.route('/hello')
def a():
    return show_html('login.html')


@login_required
def protected_serve():
    """
    Load MLFlow UI
    :return: MLFLow UI HTML
    """
    return send_from_directory(STATIC_DIR, 'index.html')


CREATE_GLOBAL_JINJA_VARIABLES: APP.context_processor = APP.context_processor(
    fix_params(create_global_jinja_variables, CLOUD_ENVIRONMENT))
LOGIN: APP.route = APP.route('/login', methods=['GET', 'POST'])(login)
LOGOUT: APP.route = APP.route('/logout')(logout)
USER_LOADER: APP.route = LOGIN_MANAGER.user_loader(user_loader)
UNAUTHORIZED_USER: APP.route = LOGIN_MANAGER.unauthorized_handler(unauthorized_user)

APP.view_functions['serve'] = protected_serve

if __name__ == "__main__":
    APP.run(host='0.0.0.0', port=5000)
