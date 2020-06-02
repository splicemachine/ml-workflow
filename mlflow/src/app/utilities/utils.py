"""
Shared utilities between mlflow and director applications
"""

from flask import Response, render_template as show_html,  request, \
    redirect, url_for
from flask_login import LoginManager, login_user, logout_user, login_required, current_user

from mlmanager_lib.database.handlers import KnownHandlers, HandlerNames
from mlmanager_lib import CloudEnvironment
from mlmanager_lib.rest.authentication import Authentication, User



LOGIN_HTML = retrieve_login_html()


def create_global_jinja_variables(cloud_environment: CloudEnvironment):
    """
    Create a dictionary of global Jinja2
    variables that can be accessed in any
    template

    :return: (dict) Dictionary of key/values
        mapping global variables to the corresponding
        Jinja Variables
    """
    return dict(
        cloud_environment_name=cloud_environment.name,
        known_handlers=KnownHandlers,
        handler_names=HandlerNames,
        can_deploy=cloud_environment.can_deploy
    )


def login() -> Response:
    """
    Show Login HTML to users if GET,
    otherwise validate credentials against DB
    :return:
    """
    if request.method == 'GET':
        if current_user.is_authenticated:
            return redirect('/')
        return show_html('login.html')

    username: str = request.form['user']
    # check against Zeppelin (Apache Shiro into DB)
    if Authentication.validate_auth(username, request.form['pw']):
        user: User = User(username)
        login_user(user)
        return redirect(request.args.get("next") or redirect('/'))

    return show_html(LOGIN_HTML, unauthorized=True)


@login_required
def logout() -> redirect:
    """
    Logout the current logged in
    user
    :returns: (redirect) redirect to login
    """
    logout_user()
    return redirect(url_for('login'))


# Login Configuration
def user_loader(username: str) -> User:
    """
    Return a user from the Session

    :param username: (str) user to form object from
    :return: (User) constructed user object
    """
    return User(username)


def unauthorized_user():
    """
    Return redirect to login
    if user is unauthorized
    :return: (redirect) redirect in browser to /login
    """
    if current_user.is_authenticated:
        return redirect('/')
    return redirect(url_for('login'))
